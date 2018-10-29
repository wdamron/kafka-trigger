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
	"bytes"
	"context"
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
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
)

// TODO (wdamron): Integrate retry configuration with Function spec
const (
	maxSendAttempts          = 0
	minSendRetryDelay        = 200 * time.Millisecond
	maxSendRetryDelay        = 60 * time.Second
	sendRetryDelayMultiplier = 1.5
	sendRetryDelayJitter     = 0.1 // should be a value in the range (0.0, 1.0]

	requestTimeout = 30 * time.Minute

	funcPort = "8080"

	logHeaders = true
)

var (
	stopM         map[string](chan struct{})
	stoppedM      map[string](chan struct{})
	consumers     map[string]bool
	consumersLock sync.Mutex

	brokers string
	config  *cluster.Config

	UserAgent = "" // filled in during linking

	httpClient          *http.Client
	headerValueReplacer = strings.NewReplacer("\r", "", "\n", "")
)

func init() {
	if sendRetryDelayJitter <= 0.0 || sendRetryDelayJitter > 1.0 {
		logrus.Fatal("kafka-consumer: sendRetryDelayJitter should be a value in the range (0.0, 1.0]")
	}

	stopM = make(map[string](chan struct{}))
	stoppedM = make(map[string](chan struct{}))
	consumers = make(map[string]bool)

	// Init config
	// taking brokers from env var
	brokers = os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka.kubeless:9092"
	}
	config = cluster.NewConfig()

	config.Consumer.MaxProcessingTime = requestTimeout
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.CommitInterval = time.Second
	config.Consumer.Return.Errors = true
	config.Group.Mode = cluster.ConsumerModePartitions
	config.Version = sarama.V1_1_0_0 // Headers are only supported in version 0.11+; see https://github.com/Shopify/sarama/blob/35324cf48e33d8260e1c7c18854465a904ade249/consumer.go#L19

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

	defaultTransport := http.DefaultTransport.(*http.Transport)
	transport := *defaultTransport
	transport.MaxIdleConns = 512
	transport.MaxIdleConnsPerHost = 128
	httpClient = &http.Client{Transport: &transport, Timeout: requestTimeout}

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
	logger := logrus.StandardLogger().WithFields(logrus.Fields{"topic": topic, "consumer-group": consumerGroupID, "function": funcName})

	groupConsumer, err := cluster.NewConsumer(brokersSlice, consumerGroupID, topicsSlice, config)
	if err != nil {
		logger.WithField("err", err).Errorf("Failed to start group-consumer")
	}

	defer func() {
		logger.Infof("Closing group-consumer and all partition-consumers (if not already closed)")
		if err = groupConsumer.Close(); err != nil {
			logger.WithField("err", err).Errorf("Error closing group-consumer and all partition-consumers")
		}
	}()

	logger.Infof("Started group-consumer")

	assignments := groupConsumer.Partitions()
	for {
		select {
		case partitionConsumer, ok := <-assignments:
			if !ok {
				return
			}
			partitionLogger := logger.WithFields(logrus.Fields{"thread": nextThreadId, "topic": partitionConsumer.Topic(), "partition": partitionConsumer.Partition(), "function": funcName})
			// Start a separate goroutine per partition to consume messages:
			go createPartitionConsumerProcess(partitionLogger, partitionConsumer, nextThreadId, funcName, ns, clientset, stopchan)
			nextThreadId++

		case <-stopchan:
			return
		}
	}
}

func createPartitionConsumerProcess(
	logger *logrus.Entry,
	consumer cluster.PartitionConsumer,
	threadId uint64,
	funcName, ns string,
	clientset kubernetes.Interface,
	stopchan chan struct{}) {

	logger.Infof("Started partition-consumer")

	topic := consumer.Topic()
	consumerContext, cancelConsumerContext := context.WithCancel(context.Background())
	defer func() {
		cancelConsumerContext()
		logger.Infof("Partition-consumer closed")
	}()

	go func() {
		<-stopchan
		cancelConsumerContext()
	}()

MessageLoop:
	for {
		select {
		case <-consumerContext.Done():
			return
		case msg, more := <-consumer.Messages():
			if !more {
				return
			}
			msgLogger := logger.WithFields(logrus.Fields{"offset": msg.Offset, "key": string(msg.Key)})
			if logHeaders && len(msg.Headers) > 0 {
				headers := logrus.Fields{}
				for _, hdr := range msg.Headers {
					if len(hdr.Value) != 0 {
						headers[string(hdr.Key)] = string(hdr.Value)
					}
				}
				msgLogger.WithFields(headers).Infof("[%s/%d/%d] Received message", topic, msg.Partition, msg.Offset)
			} else {
				msgLogger.Infof("[%s/%d/%d] Received message", topic, msg.Partition, msg.Offset)
			}

			if len(msg.Value) == 0 {
				msgLogger.Errorf("[%s/%d/%d] Skipped sending message with missing contents", topic, msg.Partition, msg.Offset)
				continue
			}

			var lastDelay time.Duration
			sendAttempts := 0
			startTime := time.Now()

			baseReq, err := buildRequest(funcName, ns, msg)
			if err != nil {
				msgLogger.WithField("err", err).Errorf("Unable to elaborate request")
				continue
			}

			for {
				select {
				case <-consumerContext.Done():
					return
				default:
				}

				reqStart := time.Now()
				reqContext, cancelReqContext := context.WithTimeout(consumerContext, requestTimeout)
				req := baseReq.WithContext(reqContext)
				if sendAttempts > 0 {
					req.Body = ioutil.NopCloser(bytes.NewReader(msg.Value))
				}
				status, body, err := sendMessage(httpClient, req.WithContext(reqContext))
				cancelReqContext()
				sendAttempts++
				reqEnd := time.Now()

				if err == nil {
					msgLogger.WithFields(logrus.Fields{"attempts": sendAttempts, "duration": reqEnd.Sub(reqStart), "total-duration": reqEnd.Sub(startTime)}).
						Infof("[%s/%d/%d] Sent message to function successfully", topic, msg.Partition, msg.Offset)

					consumer.MarkOffset(msg.Offset, "")
					continue MessageLoop
				}

				msgLogger.WithFields(logrus.Fields{
					"attempts":       sendAttempts,
					"duration":       reqEnd.Sub(reqStart),
					"total-duration": reqEnd.Sub(startTime),
					"err":            err,
					"status-code":    status,
					"response":       body,
				}).Errorf("[%s/%d/%d] Failed to send message to function", topic, msg.Partition, msg.Offset)

				if status == 409 {
					continue MessageLoop
				}
				if maxSendAttempts > 0 && sendAttempts >= maxSendAttempts {
					msgLogger.Errorf("Skipped sending message to function after %v attempts", sendAttempts)

					consumer.MarkOffset(msg.Offset, "")
					continue MessageLoop
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

				msgLogger.WithField("delay", delay.String()).Infof("[%s/%d/%d] Delaying before re-sending message", topic, msg.Partition, msg.Offset)

				select {
				case <-consumerContext.Done():
					return
				case <-time.After(delay):
					lastDelay = delay
				}
			}

		case err, more := <-consumer.Errors():
			if more {
				logger.WithField("err", err.Error()).Errorf("Partition-consumer error")
			}
		}
	}
}

// CreateKafkaConsumer creates a goroutine that subscribes to Kafka topic
func CreateKafkaConsumer(triggerObjName, funcName, ns, topic string, clientset kubernetes.Interface) {
	consumerID := generateUniqueConsumerGroupID(triggerObjName, funcName, ns, topic)
	consumersLock.Lock()
	defer consumersLock.Unlock()
	logger := logrus.StandardLogger().WithFields(logrus.Fields{"topic": topic, "function": funcName, "trigger": triggerObjName})
	if !consumers[consumerID] {
		logger.Infof("Starting group-consumer")
		stopM[consumerID] = make(chan struct{})
		stoppedM[consumerID] = make(chan struct{})
		go createConsumerProcess(brokers, topic, funcName, ns, consumerID, clientset, stopM[consumerID], stoppedM[consumerID])
		consumers[consumerID] = true
	} else {
		logger.Infof("Group-consumer already exists; skipping")
	}
}

// DeleteKafkaConsumer deletes goroutine created by CreateKafkaConsumer
func DeleteKafkaConsumer(triggerObjName, funcName, ns, topic string) {
	consumerID := generateUniqueConsumerGroupID(triggerObjName, funcName, ns, topic)
	consumersLock.Lock()
	defer consumersLock.Unlock()
	logger := logrus.StandardLogger().WithFields(logrus.Fields{"topic": topic, "function": funcName, "trigger": triggerObjName})
	if consumers[consumerID] {
		logger.Infof("Stopping/deleting group-consumer")
		// delete consumer process
		close(stopM[consumerID])
		<-stoppedM[consumerID]
		delete(consumers, consumerID)
		logger.Infof("Stopped/deleted group-consumer")
	} else {
		logger.Infof("No matching group-consumer to stop/delete; skipping")
	}
}

func generateUniqueConsumerGroupID(triggerObjName, funcName, ns, topic string) string {
	return ns + "_" + triggerObjName + "_" + funcName + "_" + topic
}

func buildRequest(funcName, namespace string, msg *sarama.ConsumerMessage) (*http.Request, error) {
	url := fmt.Sprintf("http://%s.%s.svc.cluster.local:%s", funcName, namespace, funcPort)
	req, err := http.NewRequest("POST", url, bytes.NewReader(msg.Value))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", UserAgent)
	req.Header.Add("X-Kafka-Topic", msg.Topic)
	req.Header.Add("X-Kafka-Partition", strconv.Itoa(int(msg.Partition)))
	req.Header.Add("X-Kafka-Offset", strconv.Itoa(int(msg.Offset)))
	if len(msg.Key) > 0 {
		req.Header.Add("X-Kafka-Message-Key", headerValueReplacer.Replace(string(msg.Key)))
	}
	req.Header.Add("X-Kafka-Timestamp", msg.Timestamp.Format(time.RFC3339Nano))
	if len(msg.Headers) != 0 {
		for _, hdr := range msg.Headers {
			req.Header.Add("X-Attr-"+string(hdr.Key), headerValueReplacer.Replace(string(hdr.Value)))
		}
	}

	return req, nil
}

func sendMessage(client *http.Client, req *http.Request) (int, string, error) {
	resp, err := client.Do(req)
	status := -1
	if resp != nil {
		status = resp.StatusCode
	}
	if err != nil {
		return status, "", err
	}
	if status != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return status, string(body), fmt.Errorf("Received non-200 response")
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	return 200, "", nil
}
