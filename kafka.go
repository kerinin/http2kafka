package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"code.google.com/p/go-uuid/uuid"
	"github.com/Shopify/sarama"
)

type Kafka struct {
	ProduceTopic  string
	ConsumeTopic  string
	ConsumerGroup string

	Client   *sarama.Client
	Producer *sarama.Producer
}

type ConsumeHandler func(*sarama.ConsumerEvent)

func NewKafka(brokers []string, produceTopic string, consumeTopic string, consumerGroup string) Kafka {
	id := uuid.NewRandom().String()

	client, err := sarama.NewClient(id, brokers, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Kafka Client connected")
	}

	producer, err := sarama.NewProducer(client, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("Kafka Producer created")
	}

	return Kafka{produceTopic, consumeTopic, consumerGroup, client, producer}
}

func (k *Kafka) Produce(key, val sarama.Encoder) {
	err := k.Producer.SendMessage(k.ProduceTopic, key, val)

	if err != nil {
		panic(err)
	}
}

func (k *Kafka) Consume(f ConsumeHandler) {
	ps, err := k.Client.Partitions(k.ConsumeTopic)

	if err != nil {
		panic(err)
	}

	for p := range ps {
		go k.consumePartition(int32(p), f)
	}
}

func (k *Kafka) consumePartition(p int32, f ConsumeHandler) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	consumer, err := sarama.NewConsumer(k.Client, k.ConsumeTopic, p, k.ConsumerGroup, sarama.NewConsumerConfig())
	defer func() {
		err := consumer.Close()

		if err != nil {
			fmt.Println("Closed Kafka Consumer", p)
		} else {
			fmt.Println("Error closing Kafka Consumer", p, err)
		}
	}()

	if err != nil {
		panic(err)
	} else {
		fmt.Println("Kafka Consumer Ready", p)
	}

ConsumeLoop:
	for {
		select {
		case event := <-consumer.Events():
			if event.Err != nil {
				panic(event.Err)
			}

			f(event)
		case s := <-sigs:
			fmt.Println("Kafka Consumer received signal", p, s)
			break ConsumeLoop
		}
	}
}

func (k *Kafka) Close() {
	fmt.Println("Closing Kafka Client")
	k.Client.Close()

	fmt.Println("Closing Kafka Producer")
	k.Producer.Close()
}
