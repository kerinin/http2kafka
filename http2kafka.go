package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/Shopify/sarama"
	"github.com/stretchr/graceful"
)

const TimeoutStatus = 500
const TimeoutResponse = "Request timed out.  Sorry dude"

type UniqueRequest struct {
	UUID string
	http.Request
}

type Response struct {
	UUID   string
	Status int
	Reply  string
}

func main() {
	// Parse options
	kafkaHost := flag.String("kafka-host", "localhost", "Kafka broker to connect to")
	requestTopic := flag.String("request-topic", "requests", "Kafka topic incoming request will be written to")
	responseTopic := flag.String("response-topic", "responses", "Kafka topic outgoing responses will be written to")
	consumerGroup := flag.String("consumer-group", "http2kafka", "Consumer group for tracking offsets")
	flag.Parse()
	fmt.Printf("Starting http2kafka with args: kafka-host:%v, request-topic:%v, response-topic:%v, consumer-group:%v\n", *kafkaHost, *requestTopic, *responseTopic, *consumerGroup)

	hosts := []string{*kafkaHost}
	q := NewQueue()
	k := NewKafka(hosts, *requestTopic, *responseTopic, *consumerGroup)
	defer k.Close()

	go k.Consume(func(e *sarama.ConsumerEvent) {
		var r Response
		json.Unmarshal(e.Value, r)
		q.Dequeue(r.UUID, r)
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("HTTP Request received")

		id := uuid.NewRandom().String()
		ur := UniqueRequest{id, *r}

		c := make(chan Response)
		defer q.Delete(id)

		requestJSON, err := json.Marshal(ur)
		if err != nil {
			panic(err)
		}

		fmt.Println("Request ready to log:", string(requestJSON))

		q.Enqueue(id, c)
		k.Produce(nil, sarama.StringEncoder(requestJSON))

		fmt.Println("Request written, waiting for response")
		// Block until response is dequeued or times out
		select {
		case response := <-c:
			fmt.Println("Response received, writing", id)
			w.WriteHeader(response.Status)
			w.Write([]byte(response.Reply))

		case <-time.After(5 * time.Second):
			fmt.Println("Timeout, giving up on ", id)
			w.WriteHeader(TimeoutStatus)
			w.Write([]byte(TimeoutResponse))
		}
	})
	graceful.Run(":8080", 1*time.Second, mux)
	fmt.Println("HTTP Shutdown")

	k.Wait()

	fmt.Println("At end of main, exiting")
}
