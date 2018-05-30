package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/octarinesec/confluent-kafka-go/kafka"
)

var config Config

type Config struct {
	Topic     string `default:"recreation"`
	KafkaName string `default:"kafka-191"`
	KafkaPort string `default:"9092"`
}

func getConfig(app_name string, s interface{}) error {
	err := envconfig.Process(app_name, s)
	return err
}

type MessageConfig struct {
	Broker   string
	Group    string
	Topic    string
	Producer *kafka.Producer
	Consumer *kafka.Consumer
}

func (mc *MessageConfig) connect() error {
	var err error

	config := &kafka.ConfigMap{
		"bootstrap.servers":    mc.Broker,
		"group.id":             mc.Group,
		"session.timeout.ms":   6000,
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "latest"},
		// "debug":                "fetch",
	}

	mc.Consumer, err = kafka.NewConsumer(config)
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		return err
	}

	err = mc.Consumer.SubscribeTopics([]string{mc.Topic}, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe to topic: %s\n", err)
		return err
	}

	mc.Producer, err = kafka.NewProducer(config)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return err
	}

	return nil
}

func (mc *MessageConfig) startTraffic() {
	producerClose := make(chan bool)
	consumerClose := make(chan bool)
	go mc.produce(producerClose)
	go mc.consume(consumerClose)

	fmt.Print("Traffic started, press Enter to exit.")
	bufio.NewReader(os.Stdin).ReadBytes('\n')

	producerClose <- true
	consumerClose <- true
}

func (mc *MessageConfig) consume(close chan bool) {
	run := true
	for run == true {
		select {
		case <-close:
			run = false
			mc.Consumer.Close()
		default:
			ev := mc.Consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("Message on topic %s\n", *e.TopicPartition.Topic)
				fmt.Printf("Got message: %v\n", string(e.Value))
			case kafka.PartitionEOF:
				log.Printf("Reached %v\n", e)
			case kafka.Error:
				log.Printf("ERROR: %v\n", e)
				run = false
			default:
				log.Printf("Ignored %v\n", e)
			}
		}
	}
}

func (mc *MessageConfig) produce(close chan bool) {
	run := true
	i := 0
	for run == true {
		select {
		case <-close:
			run = false
			mc.Producer.Close()
		default:
			i++
			partition := int32(0)
			message := fmt.Sprintf("Message %d on partition %d.", i, partition)
			mc.postMessage([]byte(message), partition)
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (mc *MessageConfig) postMessage(message []byte, partition int32) error {
	if partition < 0 {
		partition = kafka.PartitionAny
	}
	m := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &mc.Topic,
			Partition: partition,
		},
		Value: message,
	}
	fmt.Printf("Sending message: %v\n", string(message))
	mc.Producer.Produce(&m, nil)

	return m.TopicPartition.Error
}

func main() {
	var err error

	err = getConfig("confluent-kafka-go-191", &config)
	if err != nil {
		fmt.Printf("Error getting config values: %v\n", err)
		return
	}

	mc := MessageConfig{
		Broker: fmt.Sprintf("%s:%s", config.KafkaName, config.KafkaPort),
		Group:  "cg1",
		Topic:  config.Topic,
	}

	if err = mc.connect(); err != nil {
		fmt.Printf("Error connecting: %v", err)
		return
	}

	mc.startTraffic()
}
