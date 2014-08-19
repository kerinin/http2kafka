package main

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("http2kafka.main")
var logFormat = logging.MustStringFormatter("%{level} %{message}")

func main() {
	logging.SetFormatter(logFormat)
	logging.SetLevel(logging.DEBUG, "http2kafka.main")

	// Parse options

	c := NewConfigFromEnv()
	q := NewQueue()
	k := NewKafka(c.KafkaHosts, c.RequestTopic, c.ResponseTopic, c.ConsumerGroup)
	s := NewServer(c, q, k)

	defer k.Close()

	log.Info("Starting with config %v", c)

	log.Info("Starting Kafka %v", k)
	k.Add(1)
	go k.Consume(func(e *sarama.ConsumerEvent) {
		var r Response
		json.Unmarshal(e.Value, r)
		q.Dequeue(r.UUID, r)
	})

	log.Info("Starting Server %v", s)
	s.Add(1)
	go s.Serve()

	k.Wait()
	s.Wait()

	log.Debug("Goodbye!")
}
