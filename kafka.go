package main

import (
	"os"
	"os/signal"
	"sync"
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

	sync.WaitGroup
}

type ConsumeHandler func(*sarama.ConsumerEvent)

func NewKafka(brokers []string, produceTopic string, consumeTopic string, consumerGroup string) Kafka {
	id := uuid.NewRandom().String()

	client, err := sarama.NewClient(id, brokers, nil)
	if err != nil {
		panic(err)
	} else {
		log.Info("Kafka Client connected")
	}

	producer, err := sarama.NewProducer(client, nil)
	if err != nil {
		panic(err)
	} else {
		log.Info("Kafka Producer created")
	}

	return Kafka{produceTopic, consumeTopic, consumerGroup, client, producer, *new(sync.WaitGroup)}
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
		k.Add(1)
		go k.consumePartition(int32(p), f)
	}
	k.Done()
}

func (k *Kafka) consumePartition(p int32, f ConsumeHandler) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	consumer, err := sarama.NewConsumer(k.Client, k.ConsumeTopic, p, k.ConsumerGroup, sarama.NewConsumerConfig())
	defer func() {
		err := consumer.Close()

		if err != nil {
			log.Error("Error %v closing Kafka Consumer %v", err, p)
		} else {
			log.Info("Closed Kafka Consumer %v", p)
		}

		k.Done()
	}()

	if err != nil {
		panic(err)
	}

	log.Info("Kafka Consumer %v Ready", p)
ConsumeLoop:
	for {
		select {
		case event := <-consumer.Events():
			if event.Err != nil {
				panic(event.Err)
			}

			f(event)
		case s := <-sigs:
			log.Info("Kafka Consumer %v received signal %v", p, s)
			break ConsumeLoop
		}
	}
}

func (k *Kafka) Close() {
	log.Info("Closing Kafka Client")
	k.Client.Close()

	log.Info("Closing Kafka Producer")
	k.Producer.Close()
}
