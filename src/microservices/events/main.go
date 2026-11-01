// Command events is the CinemaAbyss events MVP service.
//
// It exposes a small HTTP API that publishes movie, user and payment events to
// Kafka topics and, in the same process, consumes those topics back and writes
// every processed event to the service log. The point of the service is to
// check how cheaply Kafka fits into the existing architecture: one binary, one
// producer, one consumer group, no extra storage.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	topicMovie   = "movie-events"
	topicUser    = "user-events"
	topicPayment = "payment-events"

	consumerGroup = "cinemaabyss-events"

	// dialTimeout bounds how long the service waits for Kafka at startup. The
	// broker is usually slower than the app in compose and in Kubernetes, so
	// failing on the first attempt only produces a restart loop.
	dialTimeout = 90 * time.Second
	dialBackoff = 3 * time.Second
)

// Event payloads. Required fields are pointers so that a missing field can be
// told apart from a legitimate zero value, and answered with 400 instead of a
// silently broken event.
type movieEvent struct {
	MovieID     *int     `json:"movie_id"`
	Title       *string  `json:"title"`
	Action      *string  `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

type userEvent struct {
	UserID    *int    `json:"user_id"`
	Username  *string `json:"username,omitempty"`
	Email     *string `json:"email,omitempty"`
	Action    *string `json:"action"`
	Timestamp *string `json:"timestamp"`
}

type paymentEvent struct {
	PaymentID  *int     `json:"payment_id"`
	UserID     *int     `json:"user_id"`
	Amount     *float64 `json:"amount"`
	Status     *string  `json:"status"`
	Timestamp  *string  `json:"timestamp"`
	MethodType *string  `json:"method_type,omitempty"`
}

func (e movieEvent) validate() string {
	switch {
	case e.MovieID == nil:
		return "movie_id is required"
	case e.Title == nil || *e.Title == "":
		return "title is required"
	case e.Action == nil || *e.Action == "":
		return "action is required"
	}
	return ""
}

func (e userEvent) validate() string {
	switch {
	case e.UserID == nil:
		return "user_id is required"
	case e.Action == nil || *e.Action == "":
		return "action is required"
	case e.Timestamp == nil || *e.Timestamp == "":
		return "timestamp is required"
	}
	return ""
}

func (e paymentEvent) validate() string {
	switch {
	case e.PaymentID == nil:
		return "payment_id is required"
	case e.UserID == nil:
		return "user_id is required"
	case e.Amount == nil:
		return "amount is required"
	case e.Status == nil || *e.Status == "":
		return "status is required"
	case e.Timestamp == nil || *e.Timestamp == "":
		return "timestamp is required"
	}
	return ""
}

// producer publishes one event and returns where it landed.
type producer struct {
	sync sarama.SyncProducer
}

func newProducer(brokers []string) (*producer, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = "cinemaabyss-events"
	// The compose stack runs Kafka 2.7; asking for a compatible protocol
	// version keeps the client from negotiating features the broker lacks.
	cfg.Version = sarama.V2_1_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 5

	sync, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &producer{sync: sync}, nil
}

func (p *producer) publish(topic, key string, payload []byte) (int32, int64, error) {
	return p.sync.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(payload),
		Timestamp: time.Now(),
	})
}

// consumerGroupHandler logs every consumed event. It is the read half of the
// MVP: the service publishes to Kafka and reads its own messages back.
type consumerGroupHandler struct {
	ready chan struct{}
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("consumed event topic=%s partition=%d offset=%d key=%s value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		session.MarkMessage(msg, "")
	}
	return nil
}

// startConsumer joins the group and keeps rejoining after rebalances until the
// context is cancelled.
func startConsumer(ctx context.Context, brokers []string, topics []string) error {
	cfg := sarama.NewConfig()
	cfg.ClientID = "cinemaabyss-events"
	cfg.Version = sarama.V2_1_0_0
	cfg.Consumer.Return.Errors = true
	// Read from the beginning: the MVP is inspected through the service log and
	// Kafka UI, so losing events produced before the group joined would leave
	// the screenshots empty.
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(brokers, consumerGroup, cfg)
	if err != nil {
		return err
	}

	go func() {
		for err := range group.Errors() {
			log.Printf("consumer error: %v", err)
		}
	}()

	go func() {
		defer group.Close()
		for ctx.Err() == nil {
			handler := &consumerGroupHandler{ready: make(chan struct{})}
			if err := group.Consume(ctx, topics, handler); err != nil && ctx.Err() == nil {
				log.Printf("consumer group error: %v", err)
				time.Sleep(dialBackoff)
				continue
			}
			select {
			case <-handler.ready:
				log.Printf("consumer group joined, topics: %s", strings.Join(topics, ", "))
			case <-ctx.Done():
			}
		}
	}()

	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("failed to write response: %v", err)
	}
}

func badRequest(w http.ResponseWriter, reason string) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": reason})
}

// publishHandler turns an HTTP call into an event. The response reports where
// the record landed, which is the easiest way to prove the write happened.
func publishHandler(p *producer, topic string, decode func([]byte) ([]byte, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}

		raw := make([]byte, 0, 1024)
		buf := make([]byte, 4096)
		for {
			n, err := r.Body.Read(buf)
			raw = append(raw, buf[:n]...)
			if err != nil {
				break
			}
		}

		payload, reason := decode(raw)
		if reason != "" {
			log.Printf("rejected %s event: %s", topic, reason)
			badRequest(w, reason)
			return
		}

		key := eventKey(topic, payload)
		partition, offset, err := p.publish(topic, key, payload)
		if err != nil {
			log.Printf("failed to publish to %s: %v", topic, err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to publish event"})
			return
		}

		log.Printf("published event topic=%s partition=%d offset=%d key=%s", topic, partition, offset, key)
		writeJSON(w, http.StatusCreated, map[string]any{
			"status":    "success",
			"topic":     topic,
			"partition": partition,
			"offset":    offset,
		})
	}
}

// eventKey uses the event identifier as the message key so all events of one
// entity keep their order within a partition.
func eventKey(topic string, payload []byte) string {
	var probe struct {
		MovieID   *int `json:"movie_id"`
		UserID    *int `json:"user_id"`
		PaymentID *int `json:"payment_id"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		return topic
	}
	switch {
	case probe.PaymentID != nil:
		return "payment:" + strconv.Itoa(*probe.PaymentID)
	case probe.MovieID != nil:
		return "movie:" + strconv.Itoa(*probe.MovieID)
	case probe.UserID != nil:
		return "user:" + strconv.Itoa(*probe.UserID)
	}
	return topic
}

func decodeMovie(raw []byte) ([]byte, string) {
	var e movieEvent
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, "invalid json: " + err.Error()
	}
	if reason := e.validate(); reason != "" {
		return nil, reason
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return nil, "invalid json: " + err.Error()
	}
	return payload, ""
}

func decodeUser(raw []byte) ([]byte, string) {
	var e userEvent
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, "invalid json: " + err.Error()
	}
	if reason := e.validate(); reason != "" {
		return nil, reason
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return nil, "invalid json: " + err.Error()
	}
	return payload, ""
}

func decodePayment(raw []byte) ([]byte, string) {
	var e paymentEvent
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, "invalid json: " + err.Error()
	}
	if reason := e.validate(); reason != "" {
		return nil, reason
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return nil, "invalid json: " + err.Error()
	}
	return payload, ""
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func brokers() []string {
	raw := envOr("KAFKA_BROKERS", "kafka:9092")
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// waitForProducer retries the initial connection: the broker is routinely up
// later than the service, and a fatal exit would only restart the container.
func waitForProducer(brokerList []string) *producer {
	deadline := time.Now().Add(dialTimeout)
	for attempt := 1; ; attempt++ {
		p, err := newProducer(brokerList)
		if err == nil {
			log.Printf("connected to kafka brokers: %s", strings.Join(brokerList, ", "))
			return p
		}
		if time.Now().After(deadline) {
			log.Fatalf("kafka is not reachable after %s: %v", dialTimeout, err)
		}
		log.Printf("kafka not ready (attempt %d): %v", attempt, err)
		time.Sleep(dialBackoff)
	}
}

func main() {
	brokerList := brokers()
	prod := waitForProducer(brokerList)
	defer prod.sync.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	topics := []string{topicMovie, topicUser, topicPayment}
	if err := startConsumer(ctx, brokerList, topics); err != nil {
		log.Fatalf("failed to start consumer: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"status": true})
	})
	mux.Handle("/api/events/movie", publishHandler(prod, topicMovie, decodeMovie))
	mux.Handle("/api/events/user", publishHandler(prod, topicUser, decodeUser))
	mux.Handle("/api/events/payment", publishHandler(prod, topicPayment, decodePayment))

	addr := ":" + envOr("PORT", "8082")
	log.Printf("events service listening on %s, brokers: %s", addr, strings.Join(brokerList, ", "))

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
		}
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server failed: %v", err)
	}
	fmt.Println("events service stopped")
}
