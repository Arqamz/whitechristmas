package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	kafka "github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

// ─── Database layer ───────────────────────────────────────────────────────────

var (
	pgDB        *sql.DB
	mongoClient *mongo.Client
	mongoColl   *mongo.Collection
)

// initPostgres opens a connection pool to PostgreSQL and creates the events
// table if it doesn't exist. Non-fatal: logs and returns nil on failure so the
// API can still serve WebSocket traffic without a DB.
func initPostgres(connStr string) error {
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("ping: %w", err)
	}

	// Create the events table matching Spark's JDBC auto-schema.
	// Column types mirror Spark's DataFrame → JDBC mappings for PostgreSQL.
	const ddl = `
CREATE TABLE IF NOT EXISTS events (
    kafka_timestamp   TIMESTAMP,
    event_id          TEXT,
    victim_id         TEXT,
    incident_date     TEXT,
    incident_time     TEXT,
    location          TEXT,
    district          TEXT,
    injury_type       TEXT,
    severity          INTEGER,
    processed_timestamp BIGINT,
    processed_at      TIMESTAMP,
    received_lag_ms   BIGINT,
    is_alert          BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_events_processed_at ON events (processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_severity     ON events (severity);
CREATE INDEX IF NOT EXISTS idx_events_district     ON events (district);`

	if _, err := db.ExecContext(ctx, ddl); err != nil {
		db.Close()
		return fmt.Errorf("ddl: %w", err)
	}

	pgDB = db
	return nil
}

// initMongo connects to MongoDB Atlas and pins the alerts collection.
func initMongo(connStr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		return fmt.Errorf("ping: %w", err)
	}

	mongoClient = client
	mongoColl = client.Database("whitechristmas").Collection("alerts")
	return nil
}

// ─── REST handlers ────────────────────────────────────────────────────────────

// EventHistoryHandler serves GET /events/history?limit=50&offset=0
// Returns paginated events from PostgreSQL ordered newest-first.
func EventHistoryHandler() http.HandlerFunc {
	type row struct {
		EventID            string  `json:"event_id"`
		VictimID           *string `json:"victim_id"`
		IncidentDate       string  `json:"incident_date"`
		IncidentTime       *string `json:"incident_time"`
		Location           *string `json:"location"`
		District           *string `json:"district"`
		InjuryType         *string `json:"injury_type"`
		Severity           *int    `json:"severity"`
		ProcessedTimestamp int64   `json:"processed_timestamp"`
		ProcessedAt        string  `json:"processed_at"`
		IsAlert            bool    `json:"is_alert"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if pgDB == nil {
			http.Error(w, `{"error":"PostgreSQL not connected"}`, http.StatusServiceUnavailable)
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		if limit <= 0 || limit > 500 {
			limit = 50
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		rows, err := pgDB.QueryContext(ctx, `
			SELECT event_id, victim_id, incident_date, incident_time, location,
			       district, injury_type, severity, processed_timestamp,
			       processed_at, is_alert
			FROM events
			ORDER BY processed_at DESC
			LIMIT $1 OFFSET $2`, limit, offset)
		if err != nil {
			log.Printf("events/history query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var events []row
		for rows.Next() {
			var e row
			var processedAt time.Time
			if err := rows.Scan(&e.EventID, &e.VictimID, &e.IncidentDate, &e.IncidentTime,
				&e.Location, &e.District, &e.InjuryType, &e.Severity,
				&e.ProcessedTimestamp, &processedAt, &e.IsAlert); err != nil {
				continue
			}
			e.ProcessedAt = processedAt.Format(time.RFC3339)
			events = append(events, e)
		}
		if events == nil {
			events = []row{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"events": events, "limit": limit, "offset": offset})
	}
}

// DistrictSummaryHandler serves GET /districts/summary
// Returns event counts + avg severity per district from PostgreSQL.
func DistrictSummaryHandler() http.HandlerFunc {
	type districtRow struct {
		District      string  `json:"district"`
		TotalEvents   int64   `json:"total_events"`
		AvgSeverity   float64 `json:"avg_severity"`
		CriticalCount int64   `json:"critical_count"`
		LastSeen      string  `json:"last_seen"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if pgDB == nil {
			http.Error(w, `{"error":"PostgreSQL not connected"}`, http.StatusServiceUnavailable)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		rows, err := pgDB.QueryContext(ctx, `
			SELECT
				COALESCE(district, 'Unknown') AS district,
				COUNT(*) AS total_events,
				COALESCE(AVG(severity), 0) AS avg_severity,
				SUM(CASE WHEN severity >= 4 THEN 1 ELSE 0 END) AS critical_count,
				MAX(processed_at) AS last_seen
			FROM events
			GROUP BY district
			ORDER BY total_events DESC`)
		if err != nil {
			log.Printf("districts/summary query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var districts []districtRow
		for rows.Next() {
			var d districtRow
			var lastSeen time.Time
			if err := rows.Scan(&d.District, &d.TotalEvents, &d.AvgSeverity, &d.CriticalCount, &lastSeen); err != nil {
				continue
			}
			d.LastSeen = lastSeen.Format(time.RFC3339)
			districts = append(districts, d)
		}
		if districts == nil {
			districts = []districtRow{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"districts": districts})
	}
}

// TrendsHandler serves GET /analytics/trends?hours=24
// Returns hourly event counts + avg severity for the last N hours.
func TrendsHandler() http.HandlerFunc {
	type trendRow struct {
		Hour        string  `json:"hour"`
		EventCount  int64   `json:"event_count"`
		AvgSeverity float64 `json:"avg_severity"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if pgDB == nil {
			http.Error(w, `{"error":"PostgreSQL not connected"}`, http.StatusServiceUnavailable)
			return
		}
		hours, _ := strconv.Atoi(r.URL.Query().Get("hours"))
		if hours <= 0 || hours > 168 {
			hours = 24
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		rows, err := pgDB.QueryContext(ctx, `
			SELECT
				DATE_TRUNC('hour', processed_at) AS hour,
				COUNT(*) AS event_count,
				COALESCE(AVG(severity), 0) AS avg_severity
			FROM events
			WHERE processed_at > NOW() - ($1 || ' hours')::INTERVAL
			GROUP BY hour
			ORDER BY hour ASC`, hours)
		if err != nil {
			log.Printf("analytics/trends query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var trends []trendRow
		for rows.Next() {
			var t trendRow
			var hour time.Time
			if err := rows.Scan(&hour, &t.EventCount, &t.AvgSeverity); err != nil {
				continue
			}
			t.Hour = hour.Format(time.RFC3339)
			trends = append(trends, t)
		}
		if trends == nil {
			trends = []trendRow{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"trends": trends, "hours": hours})
	}
}

// AlertsRecentHandler serves GET /alerts/recent?limit=20
// Returns the most recent alert documents from MongoDB.
func AlertsRecentHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if mongoColl == nil {
			http.Error(w, `{"error":"MongoDB not connected"}`, http.StatusServiceUnavailable)
			return
		}
		limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
		if limit <= 0 || limit > 200 {
			limit = 20
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		opts := options.Find().
			SetSort(bson.D{{Key: "processed_at", Value: -1}}).
			SetLimit(limit)

		cursor, err := mongoColl.Find(ctx, bson.D{}, opts)
		if err != nil {
			log.Printf("alerts/recent query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer cursor.Close(ctx)

		var alerts []bson.M
		if err := cursor.All(ctx, &alerts); err != nil {
			http.Error(w, `{"error":"decode failed"}`, http.StatusInternalServerError)
			return
		}
		if alerts == nil {
			alerts = []bson.M{}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"alerts": alerts})
	}
}

func main() {
	// Load .env from project root (works from src/api/ or WhiteChristmas/)
	for _, path := range []string{".env", "../../.env"} {
		if err := godotenv.Load(path); err == nil {
			log.Printf("Loaded env from %s", path)
			break
		}
	}

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
	pgConnStr := os.Getenv("POSTGRES_CONN_STRING")
	mongoConnStr := os.Getenv("MONGODB_CONN_STRING")

	fmt.Printf("📊 Kafka Brokers: %s\n", kafkaBrokers)
	fmt.Printf("📖 Alerts Topic:  %s\n", alertsTopic)
	fmt.Printf("🌐 API Port:      %s\n", port)
	fmt.Println()

	// Connect to PostgreSQL
	if pgConnStr != "" {
		if err := initPostgres(pgConnStr); err != nil {
			log.Printf("⚠️  PostgreSQL unavailable: %v (REST history endpoints will be disabled)", err)
		} else {
			fmt.Println("🐘 PostgreSQL: connected")
		}
	} else {
		fmt.Println("⚠️  POSTGRES_CONN_STRING not set — history endpoints disabled")
	}

	// Connect to MongoDB
	if mongoConnStr != "" {
		if err := initMongo(mongoConnStr); err != nil {
			log.Printf("⚠️  MongoDB unavailable: %v (alerts/recent endpoint will be disabled)", err)
		} else {
			fmt.Println("🍃 MongoDB:    connected")
		}
	} else {
		fmt.Println("⚠️  MONGODB_CONN_STRING not set — alerts/recent endpoint disabled")
	}
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

	// Historical / analytics REST endpoints (backed by PostgreSQL + MongoDB)
	r.Get("/events/history", EventHistoryHandler())
	r.Get("/districts/summary", DistrictSummaryHandler())
	r.Get("/analytics/trends", TrendsHandler())
	r.Get("/alerts/recent", AlertsRecentHandler())

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
		fmt.Printf("  Health:           http://localhost:%s/health\n", port)
		fmt.Printf("  Stats:            http://localhost:%s/stats\n", port)
		fmt.Printf("  WebSocket:        ws://localhost:%s/ws\n", port)
		fmt.Printf("  Event history:    http://localhost:%s/events/history\n", port)
		fmt.Printf("  District summary: http://localhost:%s/districts/summary\n", port)
		fmt.Printf("  Hourly trends:    http://localhost:%s/analytics/trends\n", port)
		fmt.Printf("  Recent alerts:    http://localhost:%s/alerts/recent\n", port)
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

	if pgDB != nil {
		pgDB.Close()
	}
	if mongoClient != nil {
		mongoClient.Disconnect(ctx)
	}

	fmt.Println("✅ Server stopped")
}
