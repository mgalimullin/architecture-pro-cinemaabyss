package main

import (
	"context"
	"encoding/json"
	"fmt"
    "io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"github.com/google/uuid"
)

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float32 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  *string   `json:"username,omitempty"`
	Email     *string   `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType *string   `json:"method_type,omitempty"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

var kafkaBroker = "kafka:9092"
var kafkaTimeout = 5 * time.Second

func main() {
	if env := os.Getenv("KAFKA_BROKERS"); env != "" {
		kafkaBroker = env
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	log.Printf("[INFO] Starting events-service on port %s", port)
	log.Printf("[INFO] Using Kafka broker: %s", kafkaBroker)

	r := mux.NewRouter()
	r.HandleFunc("/api/events/health", healthHandler).Methods("GET")
	r.HandleFunc("/api/events/movie", makeEventHandler("movie-events", "movie")).Methods("POST")
	r.HandleFunc("/api/events/user", makeEventHandler("user-events", "user")).Methods("POST")
	r.HandleFunc("/api/events/payment", makeEventHandler("payment-events", "payment")).Methods("POST")

	addr := ":" + port
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatalf("[FATAL] server error: %v", err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("[INFO] Health check requested")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func makeEventHandler(topic string, eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		log.Printf("[INFO] Received %s event request for topic '%s'", eventType, topic)

		var payload interface{}
		switch eventType {
		case "movie":
			var e MovieEvent
			if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
				log.Printf("[ERROR] Failed to decode movie event: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(ErrorResponse{Error: "invalid JSON body"})
				return
			}
			payload = e
		case "user":
			var e UserEvent
			if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
				log.Printf("[ERROR] Failed to decode user event: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(ErrorResponse{Error: "invalid JSON body"})
				return
			}
			payload = e
		case "payment":
			var e PaymentEvent
			if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
				log.Printf("[ERROR] Failed to decode payment event: %v", err)
				log.Printf("[ERROR] request: %v", r)
				bodyBytes, err := io.ReadAll(r.Body)
				if err != nil {
					log.Printf("[ERROR] Failed to read request body: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					json.NewEncoder(w).Encode(ErrorResponse{Error: "failed to read body"})
					return
				}
				log.Printf("[ERROR] request body: %s", string(bodyBytes))
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(ErrorResponse{Error: "invalid JSON body"})
				return
			}
			payload = e
		default:
			log.Printf("[WARN] Unsupported event type: %s", eventType)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "unsupported event type"})
			return
		}

		evt := Event{
			ID:        fmt.Sprintf("%s-%s", eventType, uuid.New().String()),
			Type:      eventType,
			Timestamp: time.Now().UTC(),
			Payload:   payload,
		}

		b, err := json.Marshal(evt)
		if err != nil {
			log.Printf("[ERROR] Failed to marshal event: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "failed to marshal event"})
			return
		}

		partition, offset, err := produceMessage(topic, b)
		if err != nil {
			log.Printf("[ERROR] Failed to produce message: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
			return
		}
		log.Printf("[INFO] Produced message to topic '%s' partition=%d offset=%d", topic, partition, offset)

		readMsg, err := readMessageAt(topic, partition, offset, kafkaTimeout)
		if err != nil {
			log.Printf("[ERROR] Failed to read message from topic '%s': %v", topic, err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
			return
		}

		var returned Event
		if err := json.Unmarshal(readMsg, &returned); err != nil {
			log.Printf("[ERROR] Failed to unmarshal read message: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "failed to unmarshal read message"})
			return
		}

		resp := EventResponse{
			Status:    "success",
			Partition: partition,
			Offset:    offset,
			Event:     returned,
		}
		log.Printf("[INFO] Returning successful response for event %s", evt.ID)
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(resp)
	}
}

func produceMessage(topic string, payload []byte) (int32, int64, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Version = sarama.V2_3_0_0

	producer, err := sarama.NewSyncProducer([]string{kafkaBroker}, cfg)
	if err != nil {
		return 0, 0, err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(strconv.FormatInt(time.Now().UnixNano(), 10)),
		Value: sarama.ByteEncoder(payload),
	}

	partition, offset, err := producer.SendMessage(msg)
	return partition, offset, err
}

func readMessageAt(topic string, partition int32, offset int64, timeout time.Duration) ([]byte, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Version = sarama.V2_3_0_0

	consumer, err := sarama.NewConsumer([]string{kafkaBroker}, cfg)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return nil, err
	}
	defer pc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case msg := <-pc.Messages():
			if msg.Offset == offset {
				return msg.Value, nil
			}
		case err := <-pc.Errors():
			return nil, err.Err
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for message at offset %d (topic %s partition %d)", offset, topic, partition)
		}
	}
}