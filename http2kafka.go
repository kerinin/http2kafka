package main

import (
  "flag"
  "time"
  "net/http"
  "encoding/json"

  "code.google.com/p/go-uuid/uuid"
  "github.com/Shopify/sarama"
)

const TimeoutStatus = 500
const TimeoutResponse = "Request timed out.  Sorry dude"

type UniqueRequest struct {
  UUID string
  http.Request
}

type Response struct {
  UUID string
  Status int
  Reply string
}

// func (r UniqueRequest) Marshal(v interface{}) ([]byte, error) {
  // make JSON, ignore response writer
// }

func main() {
  // Parse options
  kafkaHost := flag.String("kafka-host", "localhost", "Kafka broker to connect to")
  requestTopic := flag.String("request-topic", "requests", "Kafka topic incoming request will be written to")
  responseTopic := flag.String("response-topic", "responses", "Kafka topic outgoing responses will be written to")
  consumerGroup := flag.String("consumer-group", "http2kafka", "Consumer group for tracking offsets")
  flag.Parse()

  hosts := []string{*kafkaHost}
  q := new(Queue)
  k := NewKafka(hosts, *requestTopic, *responseTopic, *consumerGroup)
  defer k.Close()

  go k.Consume(func(e *sarama.ConsumerEvent) {
	// r := json.Unmarshal(e.value, Response)
	// q.Dequeue(r.UUID(), r)
  })

  http.HandleFunc("*", func(w http.ResponseWriter, r *http.Request) { 
	id := uuid.NewRandom().String()
	ur := UniqueRequest{id, *r}

	c := make(chan Response)
	defer q.Delete(id)

	q.Enqueue(id, c)

	requestJSON, err := json.Marshal(ur) 
	if err != nil {
	  panic(err)
	}
	k.Produce(nil, sarama.StringEncoder(requestJSON))

	// Block until response is dequeued or times out
	select {
	case response := <-c:
	  w.WriteHeader(response.Status)
	  w.Write([]byte(response.Reply))

	case <-time.After(60 * time.Second):
	  w.WriteHeader(TimeoutStatus)
	  w.Write([]byte(TimeoutResponse))
	}
  })
  http.ListenAndServe(":8080", nil)
}
