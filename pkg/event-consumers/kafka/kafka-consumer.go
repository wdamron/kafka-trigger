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
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	kubelessutil "github.com/kubeless/kubeless/pkg/utils"
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

	funcPort = 8080

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
	config.Consumer.Offsets.CommitInterval = time.Second
	config.Consumer.Return.Errors = true
	config.Group.Mode = cluster.ConsumerModePartitions
	config.Version = sarama.V0_11_0_0 // Headers are only supported in version 0.11+; see https://github.com/Shopify/sarama/blob/35324cf48e33d8260e1c7c18854465a904ade249/consumer.go#L19

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

	nextThreadId := uint64(0)

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
			logrus.Infof("[%s/%d] Started Kafka partition-consumer: consumer-group=%s thread=%v function=%s", topic, partitionConsumer.Partition(), consumerGroupID, nextThreadId, funcName)
			wg.Add(1)
			go createPartitionConsumerProcess(groupConsumer, partitionConsumer, &wg, nextThreadId, funcName, ns, clientset, stopchan)
			nextThreadId++

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
	threadId uint64,
	funcName, ns string,
	clientset kubernetes.Interface,
	stopchan chan struct{}) {

	defer wg.Done()

	httpClient := newHTTPClient()
	headerBuffer := make([]byte, 0, 1024*32)
	replacer := strings.NewReplacer("\r", "", "\n", "")
	topic := consumer.Topic()

	// Consume messages, wait for signal to stopchan to exit
MessageLoop:
	for {
		select {
		case msg, more := <-consumer.Messages():
			if !more {
				logrus.Infof("[%s/%d] Partition-consumer closed: thread=%v function=%s", consumer.Topic(), consumer.Partition(), threadId, funcName)
				return
			}
			if logHeaders {
				if len(msg.Headers) == 0 {
					const none = "(none)"
					headerBuffer = append(headerBuffer, none...)
				} else {
					for _, hdr := range msg.Headers {
						headerBuffer = append(headerBuffer, hdr.Key...)
						headerBuffer = append(headerBuffer, '=')
						headerBuffer = append(headerBuffer, hdr.Value...)
						headerBuffer = append(headerBuffer, ' ')
					}
				}
				logrus.Infof("[%s/%d/%d] Received Kafka message: thread=%v function=%s key=%s headers: %s", consumer.Topic(), consumer.Partition(), msg.Offset, threadId, funcName, string(msg.Key), string(headerBuffer))
				headerBuffer = headerBuffer[: 0 : 1024*32]
			} else {
				logrus.Infof("[%s/%d/%d] Received Kafka message: thread=%v function=%s key=%s", consumer.Topic(), consumer.Partition(), msg.Offset, threadId, funcName, string(msg.Key))
			}

			req, err := buildRequest(clientset, funcName, ns, "kafkatriggers.kubeless.io", "POST", string(msg.Value))
			if err != nil {
				logrus.Errorf("[%s/%d/%d] Unable to elaborate request: thread=%v function=%s key=%s err=%s", consumer.Topic(), consumer.Partition(), msg.Offset, threadId, funcName, string(msg.Key), err)
				continue MessageLoop
			}

			req.Header.Add("X-Kafka-Topic", topic)
			req.Header.Add("X-Kafka-Partition", strconv.Itoa(int(msg.Partition)))
			req.Header.Add("X-Kafka-Offset", strconv.Itoa(int(msg.Offset)))
			req.Header.Add("X-Kafka-Message-Key", replacer.Replace(string(msg.Key)))
			if len(msg.Headers) != 0 {
				for _, hdr := range msg.Headers {
					req.Header.Add("X-Attr-"+string(hdr.Key), replacer.Replace(string(hdr.Value)))
				}
			}

			//forward msg to function
			var lastDelay time.Duration
			sendAttempts := 0

			for {
				if err = sendMessage(httpClient, req); err != nil {
					logrus.Errorf("[%s/%d/%d] Failed to send message to function: thread=%v function=%s key=%s err=%v", consumer.Topic(), consumer.Partition(), msg.Offset, threadId, funcName, string(msg.Key), err)
					sendAttempts++
					if sendAttempts == maxSendAttempts {
						logrus.Errorf("[%s/%d/%d] Skipped sending message to function after %v attempts: thread=%v function=%s key=%s err=%v", consumer.Topic(), consumer.Partition(), msg.Offset, sendAttempts, threadId, funcName, string(msg.Key), err)
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
					logrus.Infof("[%s/%d/%d] Delaying for %s before re-sending message: thread=%v function=%s key=%s", consumer.Topic(), consumer.Partition(), msg.Offset, delay.String(), threadId, funcName, string(msg.Key))

					select {
					case <-time.After(delay):
					case <-stopchan:
						logrus.Infof("[%s/%d/%d] Stopping consumer: thread=%v function=%s key=%s err=%v", consumer.Topic(), consumer.Partition(), msg.Offset, threadId, funcName)
						return
					}

					lastDelay = delay
					continue
				}

				logrus.Infof("[%s/%d/%d] Sent message to function successfully: thread=%v function=%s key=%s", consumer.Topic(), consumer.Partition(), msg.Offset, threadId, funcName, string(msg.Key))
				groupConsumer.MarkOffset(msg, "")
				break
			}

		case err, more := <-consumer.Errors():
			if more {
				logrus.Errorf("[%s/%d] Partition-consumer error: thread=%v function=%s err=%v", consumer.Topic(), consumer.Partition(), threadId, funcName, err)
			}
		case <-stopchan:
			logrus.Infof("[%s/%d] Stopping partition-consumer: thread=%v function=%s", consumer.Topic(), consumer.Partition(), threadId, funcName)
			if err := consumer.Close(); err != nil {
				logrus.Errorf("[%s/%d] Error while closing partition-consumer: thread=%v function=%s err=%v", consumer.Topic(), consumer.Partition(), threadId, funcName, err)
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

func newHTTPClient() *http.Client {
	// Customize the Transport to have larger connection pool
	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic(fmt.Sprintf("defaultRoundTripper not an *http.Transport"))
	}
	defaultTransport := *defaultTransportPointer // dereference it to get a copy of the struct that the pointer points to
	defaultTransport.MaxIdleConns = 100
	defaultTransport.MaxIdleConnsPerHost = 100

	return &http.Client{Transport: &defaultTransport}
}

// GetHTTPReq returns the http request object that can be used to send a event with payload to function service
func buildRequest(clientset kubernetes.Interface, funcName, namespace, eventNamespace, method, body string) (*http.Request, error) {
	req, err := http.NewRequest(method, fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", funcName, namespace, funcPort), strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("Unable to create request %v", err)
	}
	timestamp := time.Now().UTC()
	eventID, err := kubelessutil.GetRandString(11)
	if err != nil {
		return nil, fmt.Errorf("Failed to create a event-ID %v", err)
	}
	req.Header.Add("event-id", eventID)
	req.Header.Add("event-time", timestamp.String())
	req.Header.Add("event-namespace", eventNamespace)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("event-type", "application/json")
	return req, nil
}

func sendMessage(client *http.Client, req *http.Request) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("Error: received error code %d: %s", resp.StatusCode, resp.Status)
	}
	return nil
}
