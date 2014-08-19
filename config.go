package main

import (
	"flag"
)

type Config struct {
	KafkaHosts    []string
	RequestTopic  string
	ResponseTopic string
	ConsumerGroup string
}

func NewConfigFromEnv() Config {
	kafkaHost := flag.String("kafka-host", "localhost", "Kafka broker to connect to")
	requestTopic := flag.String("request-topic", "requests", "Kafka topic incoming request will be written to")
	responseTopic := flag.String("response-topic", "responses", "Kafka topic outgoing responses will be written to")
	consumerGroup := flag.String("consumer-group", "http2kafka", "Consumer group for tracking offsets")
	flag.Parse()
	hosts := []string{*kafkaHost}

	return Config{hosts, *requestTopic, *responseTopic, *consumerGroup}
}
