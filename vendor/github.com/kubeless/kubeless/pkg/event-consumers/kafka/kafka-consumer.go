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

	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
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
		logrus.Fatalf("Failed to start consumer: %v -- broker=%v topic=%s function=%s, consumer-group=%v\n", err, broker, topic, funcName, consumerGroupID)
	}
	defer groupConsumer.Close()

	logrus.Infof("Started Kakfa consumer: broker=%v topic=%s function=%s, consumer-group=%v\n", broker, topic, funcName, consumerGroupID)

	var wg sync.WaitGroup
	for {
		select {
		case partitionConsumer, ok := <-groupConsumer.Partitions():
			if !ok {
				wg.Wait()
				logrus.Fatalf("Group consumer closed unexpectedly: topic=%s function=%s, consumer-group=%v\n", topic, funcName, consumerGroupID)
				return
			}

			// Start a separate goroutine per partition to consume messages:
			logrus.Infof("Started Kafka partition-consumer: topic=%s function=%s partition=%v\n", topic, funcName, partitionConsumer.Partition())
			wg.Add(1)
			go createPartitionConsumerProcess(groupConsumer, partitionConsumer, &wg, funcName, ns, clientset, stopchan)

		case <-stopchan:
			// Wait for all partition-consumers to stop:
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

	// Consume messages, wait for signal to stopchan to exit
MessageLoop:
	for {
		select {
		case msg, more := <-consumer.Messages():
			if !more {
				logrus.Infof("Partition consumer closed unexpectedly: topic=%s function=%s partition=%v\n", consumer.Topic(), funcName, consumer.Partition())
				return
			}
			logrus.Infof("Received Kafka message: topic=%s partition=%d offset=%s key=%s value=%s\n", consumer.Topic(), consumer.Partition(), msg.Offset, string(msg.Key), string(msg.Value))
			req, err := utils.GetHTTPReq(clientset, funcName, ns, "kafkatriggers.kubeless.io", "POST", string(msg.Value))
			if err != nil {
				logrus.Errorf("Unable to elaborate request: %v -- topic=%s function=%s partition=%d offset=%s key=%s\n", err, consumer.Topic(), funcName, consumer.Partition(), msg.Offset, string(msg.Key))
				continue MessageLoop
			}

			//forward msg to function
			var lastDelay time.Duration
			sendAttempts := 0

			for {
				if err = utils.SendMessage(req); err != nil {
					logrus.Errorf("Failed to send message to function: %v -- topic=%s function=%s partition=%d offset=%s key=%s\n", err, consumer.Topic(), funcName, consumer.Partition(), msg.Offset, string(msg.Key))
					sendAttempts++
					if sendAttempts == maxSendAttempts {
						logrus.Errorf("Skipped re-sending message to function after %v attempts: %v -- topic=%s function=%s partition=%d offset=%s key=%s\n", sendAttempts, err, consumer.Topic(), funcName, consumer.Partition(), msg.Offset, string(msg.Key))
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
					logrus.Infof("Delaying for %s before re-sending message: topic=%s function=%s partition=%d offset=%s key=%s\n", delay.String(), consumer.Topic(), funcName, consumer.Partition(), msg.Offset, string(msg.Key))

					select {
					case <-time.After(delay):
					case <-stopchan:
						logrus.Infof("Stopping consumer: topic=%s function=%s partition=%d offset=%s key=%s\n", consumer.Topic(), funcName, consumer.Partition(), msg.Offset, string(msg.Key))
						return
					}

					lastDelay = delay
					continue
				}

				logrus.Infof("Sent message to function successfully: topic=%s function=%s partition=%d offset=%s key=%s\n", consumer.Topic(), funcName, consumer.Partition(), msg.Offset, string(msg.Key))
				groupConsumer.MarkOffset(msg, "")
				break
			}

		case err, more := <-consumer.Errors():
			if more {
				logrus.Errorf("Partition-consumer error: %s -- topic=%s function=%s partition=%d\n", err.Error(), consumer.Topic(), funcName, consumer.Partition())
			}
		case <-stopchan:
			logrus.Infof("Stopping consumer: topic=%s function=%s partition=%v\n", consumer.Topic(), funcName, consumer.Partition())
			if err := consumer.Close(); err != nil {
				logrus.Errorf("Error while closing partition-consumer: %v -- topic=%s function=%s partition=%v\n", err, consumer.Topic(), funcName, consumer.Partition())
			}
			return
		}
	}
}

// CreateKafkaConsumer creates a goroutine that subscribes to Kafka topic
func CreateKafkaConsumer(triggerObjName, funcName, ns, topic string, clientset kubernetes.Interface) error {
	consumerID := generateUniqueConsumerGroupID(triggerObjName, funcName, ns, topic)
	if !consumerM[consumerID] {
		logrus.Infof("Creating Kafka consumer for the function %s associated with for trigger %s", funcName, triggerObjName)
		stopM[consumerID] = make(chan struct{})
		stoppedM[consumerID] = make(chan struct{})
		go createConsumerProcess(brokers, topic, funcName, ns, consumerID, clientset, stopM[consumerID], stoppedM[consumerID])
		consumerM[consumerID] = true
		logrus.Infof("Created Kafka consumer for the function %s associated with for trigger %s", funcName, triggerObjName)
	} else {
		logrus.Infof("Consumer for function %s associated with trigger %s already exists, so just returning", funcName, triggerObjName)
	}
	return nil
}

// DeleteKafkaConsumer deletes goroutine created by CreateKafkaConsumer
func DeleteKafkaConsumer(triggerObjName, funcName, ns, topic string) error {
	consumerID := generateUniqueConsumerGroupID(triggerObjName, funcName, ns, topic)
	if consumerM[consumerID] {
		logrus.Infof("Stopping consumer for the function %s associated with for trigger %s", funcName, triggerObjName)
		// delete consumer process
		close(stopM[consumerID])
		<-stoppedM[consumerID]
		consumerM[consumerID] = false
		logrus.Infof("Stopped consumer for the function %s associated with for trigger %s", funcName, triggerObjName)
	} else {
		logrus.Infof("Consumer for function %s associated with trigger does n't exists. Good enough to skip the stop", funcName, triggerObjName)
	}
	return nil
}

func generateUniqueConsumerGroupID(triggerObjName, funcName, ns, topic string) string {
	return ns + "_" + triggerObjName + "_" + funcName + "_" + topic
}
