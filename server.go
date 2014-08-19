package main

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/Shopify/sarama"
	"github.com/stretchr/graceful"
)

type Server struct {
	TimeoutTime     time.Duration
	TimeoutStatus   int
	TimeoutResponse string

	RequestQueue Queue
	Kafka        Kafka

	sync.WaitGroup
}

type UniqueRequest struct {
	UUID string
	http.Request
}

type Response struct {
	UUID   string
	Status int
	Reply  string
}

func NewServer(c Config, q Queue, k Kafka) Server {
	return Server{
		TimeoutTime:     5 * time.Second,
		TimeoutStatus:   500,
		TimeoutResponse: "Request timed out.",
		RequestQueue:    q,
		Kafka:           k,
		// *new(sync.WaitGroup),
	}
}

func (s *Server) Handler(w http.ResponseWriter, r *http.Request) {
	log.Debug("HTTP Request received")

	id := uuid.NewRandom().String()
	ur := UniqueRequest{id, *r}

	c := make(chan Response)
	defer s.RequestQueue.Delete(id)

	requestJSON, err := json.Marshal(ur)
	if err != nil {
		panic(err)
	}

	log.Debug("Request ready to log: %v", string(requestJSON))

	s.RequestQueue.Enqueue(id, c)
	s.Kafka.Produce(nil, sarama.StringEncoder(requestJSON))

	log.Debug("Request written, waiting for response")
	// Block until response is dequeued or times out
	select {
	case response := <-c:
		log.Debug("Response received, writing %v", id)
		w.WriteHeader(response.Status)
		w.Write([]byte(response.Reply))

	case <-time.After(s.TimeoutTime):
		log.Error("Timeout, giving up on %v", id)
		w.WriteHeader(s.TimeoutStatus)
		w.Write([]byte(s.TimeoutResponse))
	}
}

func (s *Server) Serve() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.Handler)
	graceful.Run(":8080", s.TimeoutTime, mux)
	s.Done()
}
