/*
Copyright (c) 2016-2017 Bitnami

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/kubeless/kafka-trigger/pkg/utils"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
)

// TODO (wdamron): Integrate retry configuration with Function spec
const (
	maxSendAttempts          = 10
	minSendRetryDelay        = 200 * time.Millisecond
	maxSendRetryDelay        = 5 * time.Second
	sendRetryDelayMultiplier = 1.5
	sendRetryDelayJitter     = 0.1 // should be a value in the range (0.0, 1.0]

	logHeaders = true
)

var (
	stopM     map[string](chan struct{})
	stoppedM  map[string](chan struct{})
	consumerM map[string]bool
	brokers   string
	config    *cluster.Config
)

func init() {
	if sendRetryDelayJitter <= 0.0 || sendRetryDelayJitter > 1.0 {
		logrus.Fatal("kafka-consumer: sendRetryDelayJitter should be a value in the range (0.0, 1.0]")
	}

	stopM = make(map[string](chan struct{}))
	stoppedM = make(map[string](chan struct{}))
	consumerM = make(map[string]bool)

	// Init config
	// taking brokers from env var
	brokers = os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka.kubeless:9092"
	}
	config = cluster.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Group.Mode = cluster.ConsumerModePartitions

	var err error

	if enableTLS, _ := strconv.ParseBool(os.Getenv("KAFKA_ENABLE_TLS")); enableTLS {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config, err = GetTLSConfiguration(os.Getenv("KAFKA_CACERTS"), os.Getenv("KAFKA_CERT"), os.Getenv("KAFKA_KEY"), os.Getenv("KAFKA_INSECURE"))
		if err != nil {
			logrus.Fatalf("Failed to set tls configuration: %v", err)
		}
	}
	if enableSASL, _ := strconv.ParseBool(os.Getenv("KAFKA_ENABLE_SASL")); enableSASL {
		config.Net.SASL.Enable = true
		config.Version = sarama.V0_10_0_0
		config.Net.SASL.User, config.Net.SASL.Password, err = GetSASLConfiguration(os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_PASSWORD"))
		if err != nil {
			logrus.Fatalf("Failed to set SASL configuration: %v", err)
		}
	}

}

func randomDelayJitter() float64 {
	plusminus1 := rand.Float64()*2.0 - 1.0
	offset := plusminus1 * sendRetryDelayJitter
	if offset <= -1.0 {
		offset = -0.9
	}
	return 1.0 + offset
}

// createConsumerProcess gets messages to a Kafka topic from the broker and send the payload to function service
func createConsumerProcess(broker, topic, funcName, ns, consumerGroupID string, clientset kubernetes.Interface, stopchan, stoppedchan chan struct{}) {
	defer close(stoppedchan)

	// Init consumer
	brokersSlice := []string{broker}
	topicsSlice := []string{topic}

	groupConsumer, err := cluster.NewConsumer(brokersSlice, consumerGroupID, topicsSlice, config)
	if err != nil {
		logrus.Infof("[%s] Failed to start group-consumer: broker=%v consumer-group=%s function=%s err=%v", topic, broker, consumerGroupID, funcName, err)
	}
	defer groupConsumer.Close()

	logrus.Infof("[%s] Started Kakfa group-consumer: broker=%v consumer-group=%s function=%s, ", topic, broker, consumerGroupID, funcName)

	var wg sync.WaitGroup
	assignments := groupConsumer.Partitions()
	for {
		select {
		case partitionConsumer, ok := <-assignments:
			if !ok {
				wg.Wait()
				logrus.Infof("[%s] Group-consumer closed: consumer-group=%s function=%s", topic, consumerGroupID, funcName)
				return
			}

			// Start a separate goroutine per partition to consume messages:
			logrus.Infof("[%s/%d] Started Kafka partition-consumer: consumer-group=%s function=%s", topic, partitionConsumer.Partition(), consumerGroupID, funcName)
			wg.Add(1)
			go createPartitionConsumerProcess(groupConsumer, partitionConsumer, &wg, funcName, ns, clientset, stopchan)

		case <-stopchan:
			logrus.Infof("[%s] Waiting for all partition-consumers to close: consumer-group=%s function=%s", topic, consumerGroupID, funcName)
			wg.Wait()
			return
		}
	}
}

func createPartitionConsumerProcess(
	groupConsumer *cluster.Consumer,
	consumer cluster.PartitionConsumer,
	wg *sync.WaitGroup,
	funcName, ns string,
	clientset kubernetes.Interface,
	stopchan chan struct{}) {

	defer wg.Done()

	headerBuffer := make([]byte, 0, 1024*32)

	// Consume messages, wait for signal to stopchan to exit
MessageLoop:
	for {
		select {
		case msg, more := <-consumer.Messages():
			if !more {
				logrus.Infof("[%s/%d] Partition consumer closed: function=%s", consumer.Topic(), consumer.Partition(), funcName)
				return
			}
			if logHeaders {
				for _, hdr := range msg.Headers {
					headerBuffer = append(headerBuffer, hdr.Key...)
					headerBuffer = append(headerBuffer, '=')
					headerBuffer = append(headerBuffer, hdr.Value...)
					headerBuffer = append(headerBuffer, ' ')
				}

				logrus.Infof("[%s/%d/%d] Received Kafka message: function=%s key=%s headers: %s", consumer.Topic(), consumer.Partition(), msg.Offset, funcName, string(msg.Key), string(headerBuffer))
				headerBuffer = headerBuffer[:0]
			} else {
				logrus.Infof("[%s/%d/%d] Received Kafka message: function=%s key=%s", consumer.Topic(), consumer.Partition(), msg.Offset, funcName, string(msg.Key))
			}
			req, err := utils.GetHTTPReq(clientset, funcName, ns, "kafkatriggers.kubeless.io", "POST", string(msg.Value))
			if err != nil {
				logrus.Errorf("[%s/%d/%d] Unable to elaborate request: function=%s key=%s err=%s", consumer.Topic(), consumer.Partition(), msg.Offset, funcName, string(msg.Key), err)
				continue MessageLoop
			}

			//forward msg to function
			var lastDelay time.Duration
			sendAttempts := 0

			for {
				if err = utils.SendMessage(req); err != nil {
					logrus.Errorf("[%s/%d/%d] Failed to send message to function: function=%s key=%s err=%v", consumer.Topic(), consumer.Partition(), msg.Offset, funcName, string(msg.Key), err)
					sendAttempts++
					if sendAttempts == maxSendAttempts {
						logrus.Errorf("[%s/%d/%d] Skipped sending message to function after %v attempts: function=%s key=%s err=%v", consumer.Topic(), consumer.Partition(), msg.Offset, sendAttempts, funcName, string(msg.Key), err)
						groupConsumer.MarkOffset(msg, "")
						break
					}

					var delay time.Duration
					if lastDelay < minSendRetryDelay {
						delay = minSendRetryDelay
					} else {
						delay = time.Duration(float64(lastDelay) * sendRetryDelayMultiplier * randomDelayJitter())
					}
					if delay > maxSendRetryDelay {
						delay = time.Duration(float64(maxSendRetryDelay) * randomDelayJitter())
					}
					logrus.Infof("[%s/%d/%d] Delaying for %s before re-sending message: function=%s key=%s", consumer.Topic(), consumer.Partition(), msg.Offset, delay.String(), funcName, string(msg.Key))

					select {
					case <-time.After(delay):
					case <-stopchan:
						logrus.Infof("[%s/%d/%d] Stopping consumer: function=%s key=%s err=%v", consumer.Topic(), consumer.Partition(), msg.Offset, funcName)
						return
					}

					lastDelay = delay
					continue
				}

				logrus.Infof("[%s/%d/%d] Sent message to function successfully: function=%s key=%s", consumer.Topic(), consumer.Partition(), msg.Offset, funcName, string(msg.Key))
				groupConsumer.MarkOffset(msg, "")
				break
			}

		case err, more := <-consumer.Errors():
			if more {
				logrus.Errorf("[%s/%d] Partition-consumer error: function=%s err=%v", consumer.Topic(), consumer.Partition(), funcName, err)
			}
		case <-stopchan:
			logrus.Infof("[%s/%d] Stopping partition-consumer: function=%s", consumer.Topic(), consumer.Partition(), funcName)
			if err := consumer.Close(); err != nil {
				logrus.Errorf("[%s/%d] Error while closing partition-consumer: function=%s err=%v", consumer.Topic(), consumer.Partition(), funcName, err)
			}
			return
		}
	}
}

// CreateKafkaConsumer creates a goroutine that subscribes to Kafka topic
func CreateKafkaConsumer(triggerObjName, funcName, ns, topic string, clientset kubernetes.Interface) error {
	consumerID := generateUniqueConsumerGroupID(triggerObjName, funcName, ns, topic)
	if !consumerM[consumerID] {
		logrus.Infof("[%s] Creating Kafka consumer: function=%s trigger=%s", topic, funcName, triggerObjName)
		stopM[consumerID] = make(chan struct{})
		stoppedM[consumerID] = make(chan struct{})
		go createConsumerProcess(brokers, topic, funcName, ns, consumerID, clientset, stopM[consumerID], stoppedM[consumerID])
		consumerM[consumerID] = true
		logrus.Infof("[%s] Created Kafka consumer: function=%s trigger=%s", topic, funcName, triggerObjName)
	} else {
		logrus.Infof("[%s] Consumer already exists, skipping: function=%s trigger=%s", topic, funcName, triggerObjName)
	}
	return nil
}

// DeleteKafkaConsumer deletes goroutine created by CreateKafkaConsumer
func DeleteKafkaConsumer(triggerObjName, funcName, ns, topic string) error {
	consumerID := generateUniqueConsumerGroupID(triggerObjName, funcName, ns, topic)
	if consumerM[consumerID] {
		logrus.Infof("[%s] Stopping/deleting consumer: function=%s trigger=%s", topic, funcName, triggerObjName)
		// delete consumer process
		close(stopM[consumerID])
		<-stoppedM[consumerID]
		delete(consumerM, consumerID)
		logrus.Infof("[%s] Stopped/deleted consumer: function=%s trigger=%s", topic, funcName, triggerObjName)
	} else {
		logrus.Infof("[%s] No matching consumer to stop/delete, skipping: function=%s trigger=%s", topic, funcName, triggerObjName)
	}
	return nil
}

func generateUniqueConsumerGroupID(triggerObjName, funcName, ns, topic string) string {
	return ns + "_" + triggerObjName + "_" + funcName + "_" + topic
}
