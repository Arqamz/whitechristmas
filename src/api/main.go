package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

// CrimeEvent represents a crime alert forwarded from Spark via the alerts topic.
// Fields mirror the JSON written by StreamProcessor's alertsQuery.
type CrimeEvent struct {
	EventID               string  `json:"event_id"`
	VictimID              *string `json:"victim_id"`
	IncidentDate          string  `json:"incident_date"`
	IncidentTime          *string `json:"incident_time"`
	Location              *string `json:"location"`
	District              *string `json:"district"`
	InjuryType            *string `json:"injury_type"`
	Severity              *int    `json:"severity"`
	ProcessedTimestamp    int64   `json:"processed_timestamp"`
	ProcessedTimestampAPI int64   `json:"processed_timestamp_api,omitempty"`
}

// WebSocketMessage represents a message sent to WebSocket clients
type WebSocketMessage struct {
	Type      string      `json:"type"` // "event", "stats", "connection"
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}

// EventHub manages WebSocket connections and broadcasts
type EventHub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan *WebSocketMessage
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.RWMutex
	eventCount int64
}

// NewEventHub creates a new event hub
func NewEventHub() *EventHub {
	return &EventHub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan *WebSocketMessage, 100),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

// Run starts the event hub
func (h *EventHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("WebSocket client connected. Total clients: %d", len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			delete(h.clients, client)
			h.mu.Unlock()
			client.Close()
			log.Printf("WebSocket client disconnected. Total clients: %d", len(h.clients))

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case err := <-make(chan error, 1):
					if err != nil {
						h.mu.RUnlock()
						h.unregister <- client
						h.mu.RLock()
					}
				default:
					client.WriteJSON(message)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// BroadcastEvent sends an event to all connected clients
func (h *EventHub) BroadcastEvent(event *CrimeEvent) {
	h.mu.Lock()
	h.eventCount++
	h.mu.Unlock()

	msg := &WebSocketMessage{
		Type:      "event",
		Timestamp: time.Now().UnixMilli(),
		Data:      event,
	}
	h.broadcast <- msg
}

// GetConnectedClients returns the number of connected clients
func (h *EventHub) GetConnectedClients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// GetEventCount returns the total number of events processed
func (h *EventHub) GetEventCount() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.eventCount
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for now
	},
}

// WebSocketHandler handles WebSocket connections
func WebSocketHandler(hub *EventHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("WebSocket upgrade error: %v", err)
			return
		}

		hub.register <- conn

		// Send welcome message
		welcomeMsg := &WebSocketMessage{
			Type:      "connection",
			Timestamp: time.Now().UnixMilli(),
			Data: map[string]string{
				"message": "Connected to WhiteChristmas event stream",
				"version": "0.1.0",
			},
		}
		conn.WriteJSON(welcomeMsg)

		// Keep connection alive and handle any incoming messages
		go func() {
			defer func() {
				hub.unregister <- conn
			}()

			for {
				var msg map[string]interface{}
				err := conn.ReadJSON(&msg)
				if err != nil {
					if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
						log.Printf("WebSocket error: %v", err)
					}
					return
				}
				// Handle any client messages here if needed
			}
		}()
	}
}

// KafkaConsumer reads from Kafka and broadcasts events
func KafkaConsumer(hub *EventHub, kafkaBrokers, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBrokers},
		Topic:          topic,
		GroupID:        "whitechristmas-api",
		StartOffset:    kafka.LastOffset,
		CommitInterval: time.Second,
	})
	defer reader.Close()

	log.Printf("📖 Kafka consumer started for topic: %s", topic)

	for {
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Kafka read error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var event CrimeEvent
		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Printf("Failed to unmarshal event: %v", err)
			continue
		}

		hub.BroadcastEvent(&event)
		reader.CommitMessages(context.Background(), msg)

		log.Printf("📨 Event #%d broadcast: %s (severity: %v)",
			hub.GetEventCount(),
			event.EventID,
			event.Severity)
	}
}

// corsMiddleware adds CORS headers so the Next.js dev server on :3000 can reach the API on :8081.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// StatsHandler returns server statistics
func StatsHandler(hub *EventHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats := map[string]interface{}{
			"connected_clients": hub.GetConnectedClients(),
			"total_events":      hub.GetEventCount(),
			"timestamp":         time.Now().UnixMilli(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	}
}

// HealthHandler returns health status
func HealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		health := map[string]string{
			"status":  "ok",
			"name":    "WhiteChristmas Event API",
			"version": "0.1.0",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(health)
	}
}

func main() {
	// Configuration
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}

	alertsTopic := os.Getenv("ALERTS_TOPIC")
	if alertsTopic == "" {
		alertsTopic = "alerts"
	}

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8081"
	}

	fmt.Println("╔════════════════════════════════════════════════════╗")
	fmt.Println("║  🚀 WhiteChristmas Event API                      ║")
	fmt.Println("╚════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("📊 Kafka Brokers: %s\n", kafkaBrokers)
	fmt.Printf("📖 Alerts Topic:  %s\n", alertsTopic)
	fmt.Printf("🌐 API Port:      %s\n", port)
	fmt.Println()

	// Create event hub
	hub := NewEventHub()
	go hub.Run()

	// Start Kafka consumer
	go KafkaConsumer(hub, kafkaBrokers, alertsTopic)

	// Setup router
	r := chi.NewRouter()
	r.Use(corsMiddleware)

	// Routes
	r.Get("/health", HealthHandler())
	r.Get("/stats", StatsHandler(hub))
	r.HandleFunc("/ws", WebSocketHandler(hub))

	// Static files serving (optional - for serving frontend)
	r.Handle("/static/*", http.FileServer(http.Dir(".")))

	// Create server
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		fmt.Println("═════════════════════════════════════════════════════")
		fmt.Println("✅ API Server Started!")
		fmt.Println("═════════════════════════════════════════════════════")
		fmt.Println()
		fmt.Println("📡 Available endpoints:")
		fmt.Printf("  Health:    http://localhost:%s/health\n", port)
		fmt.Printf("  Stats:     http://localhost:%s/stats\n", port)
		fmt.Printf("  WebSocket: ws://localhost:%s/ws\n", port)
		fmt.Println()
		fmt.Println("Press Ctrl+C to stop...")
		fmt.Println()

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	// Graceful shutdown
	fmt.Println()
	fmt.Println()
	fmt.Println("🛑 Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Shutdown error: %v", err)
	}

	fmt.Println("✅ Server stopped")
}
