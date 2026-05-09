package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafka "github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ── Prometheus metrics ────────────────────────────────────────────────────────

var (
	// Labelled by severity (1-5) and source dataset so Grafana can fan these out
	// into per-dataset panels without additional recording rules.
	metricEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "whitechristmas_events_total",
		Help: "Total crime events received from Kafka, by severity and source dataset.",
	}, []string{"severity", "dataset"})

	metricWSClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "whitechristmas_websocket_clients",
		Help: "Number of currently connected WebSocket clients.",
	})

	// Labelled by topic so per-dataset ingestion rates are independently visible.
	metricKafkaMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "whitechristmas_kafka_messages_total",
		Help: "Total Kafka messages consumed, by topic.",
	}, []string{"topic"})

	metricHTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "whitechristmas_http_request_duration_seconds",
		Help:    "HTTP request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})

	metricBroadcastErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "whitechristmas_broadcast_errors_total",
		Help: "Total errors while broadcasting to WebSocket clients.",
	})
)

// CrimeEvent represents a unified crime alert forwarded from Spark via the alerts topics.
// Fields cover all five Chicago public safety datasets.
type CrimeEvent struct {
	EventID               string  `json:"event_id"`
	SourceDataset         *string `json:"source_dataset,omitempty"`
	VictimID              *string `json:"victim_id"`
	IncidentDate          string  `json:"incident_date"`
	IncidentTime          *string `json:"incident_time"`
	Location              *string `json:"location"`
	District              *string `json:"district"`
	Beat                  *string `json:"beat,omitempty"`
	CrimeType             *string `json:"crime_type,omitempty"`
	InjuryType            *string `json:"injury_type"`
	EventLabel            *string `json:"event_label,omitempty"`
	Severity              *int    `json:"severity"`
	Latitude              *string `json:"latitude,omitempty"`
	Longitude             *string `json:"longitude,omitempty"`
	IsArrest              *bool   `json:"is_arrest,omitempty"`
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
			metricWSClients.Inc()
			log.Printf("WebSocket client connected. Total clients: %d", len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			delete(h.clients, client)
			h.mu.Unlock()
			metricWSClients.Dec()
			_ = client.Close()
			log.Printf("WebSocket client disconnected. Total clients: %d", len(h.clients))

		case message := <-h.broadcast:
			h.mu.Lock()
			var failed []*websocket.Conn
			for client := range h.clients {
				if err := client.WriteJSON(message); err != nil {
					metricBroadcastErrors.Inc()
					failed = append(failed, client)
				}
			}
			for _, client := range failed {
				delete(h.clients, client)
				_ = client.Close()
				metricWSClients.Dec()
			}
			h.mu.Unlock()
			if len(failed) > 0 {
				log.Printf("WebSocket: removed %d dead clients. Total: %d", len(failed), func() int {
					h.mu.RLock()
					defer h.mu.RUnlock()
					return len(h.clients)
				}())
			}
		}
	}
}

// BroadcastEvent sends an event to all connected clients
func (h *EventHub) BroadcastEvent(event *CrimeEvent) {
	h.mu.Lock()
	h.eventCount++
	h.mu.Unlock()

	sev := "unknown"
	if event.Severity != nil {
		sev = strconv.Itoa(*event.Severity)
	}
	dataset := "unknown"
	if event.SourceDataset != nil {
		dataset = *event.SourceDataset
	}
	metricEventsTotal.WithLabelValues(sev, dataset).Inc()

	msg := &WebSocketMessage{
		Type:      "event",
		Timestamp: time.Now().UnixMilli(),
		Data:      event,
	}
	select {
	case h.broadcast <- msg:
	default:
		metricBroadcastErrors.Inc()
		log.Printf("WebSocket broadcast channel full, dropping event")
	}
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
		_ = conn.WriteJSON(welcomeMsg)

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

// KafkaConsumer reads from a single Kafka topic and broadcasts events.
// Launch one goroutine per topic for independent, parallel consumption.
func KafkaConsumer(hub *EventHub, kafkaBrokers, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBrokers},
		Topic:          topic,
		GroupID:        "whitechristmas-api-" + topic, // isolated consumer group per topic
		StartOffset:    kafka.LastOffset,
		CommitInterval: time.Second,
	})
	defer func() { _ = reader.Close() }()

	log.Printf("📖 Kafka consumer started for topic: %s", topic)

	for {
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("[%s] Kafka read error: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}

		var event CrimeEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("[%s] Failed to unmarshal event: %v", topic, err)
			_ = reader.CommitMessages(context.Background(), msg)
			continue
		}

		metricKafkaMessages.WithLabelValues(topic).Inc()
		hub.BroadcastEvent(&event)
		_ = reader.CommitMessages(context.Background(), msg)

		log.Printf("[%s] 📨 Event #%d broadcast: %s (severity: %v, dataset: %v)",
			topic,
			hub.GetEventCount(),
			event.EventID,
			event.Severity,
			event.SourceDataset)
	}
}

// instrumentMiddleware wraps handlers to record HTTP duration metrics.
func instrumentMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		metricHTTPDuration.WithLabelValues(
			r.Method,
			r.URL.Path,
			strconv.Itoa(rw.status),
		).Observe(time.Since(start).Seconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.status = code
	sw.ResponseWriter.WriteHeader(code)
}

// Hijack delegates to the underlying ResponseWriter so WebSocket upgrades work.
func (sw *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := sw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying ResponseWriter does not implement http.Hijacker")
	}
	return h.Hijack()
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
		_ = json.NewEncoder(w).Encode(stats)
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
		_ = json.NewEncoder(w).Encode(health)
	}
}

// ─── Database layer ───────────────────────────────────────────────────────────

var (
	pgDB        *sql.DB
	mongoClient *mongo.Client
	mongoColl   *mongo.Collection
)

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
		_ = db.Close()
		return fmt.Errorf("ping: %w", err)
	}

	// DDL covers all five Chicago public safety datasets via the unified Avro schema.
	// alerts table includes source_dataset for per-dataset anomaly tracking.
	const ddl = `
CREATE TABLE IF NOT EXISTS crime_trends (
    year         INTEGER,
    month        INTEGER,
    day_of_week  INTEGER,
    hour         INTEGER,
    crime_count  BIGINT
);
CREATE TABLE IF NOT EXISTS top_arrest_rate_crime_types (
    primary_type  TEXT,
    total_crimes  BIGINT,
    total_arrests BIGINT,
    arrest_rate   DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS violence_analysis (
    month                      INTEGER,
    district                   TEXT,
    homicides                  BIGINT,
    non_fatal_shootings        BIGINT,
    gunshot_incidents          BIGINT,
    total_incidents            BIGINT,
    gunshot_proportion_overall DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS hotspots (
    cluster_id    INTEGER,
    crime_count   BIGINT,
    avg_lat       DOUBLE PRECISION,
    avg_lon       DOUBLE PRECISION,
    primary_types TEXT,
    centroid_lat  DOUBLE PRECISION,
    centroid_lon  DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS correlations (
    correlation_type   TEXT,
    group_key          TEXT,
    community_area     TEXT,
    total_crimes       BIGINT,
    total_arrests      BIGINT,
    metric_a           DOUBLE PRECISION,
    violence_incidents BIGINT,
    metric_b           DOUBLE PRECISION,
    sex_offender_count BIGINT
);
CREATE TABLE IF NOT EXISTS sex_offender_proximity (
    offender_name   TEXT,
    block           TEXT,
    race            TEXT,
    gender          TEXT,
    victim_minor    TEXT,
    is_victim_minor BOOLEAN,
    priority_flag   TEXT
);
CREATE TABLE IF NOT EXISTS district_offender_density (
    district                    TEXT,
    district_name               TEXT,
    station_address             TEXT,
    station_lat                 DOUBLE PRECISION,
    station_lon                 DOUBLE PRECISION,
    total_registered_offenders  BIGINT,
    minor_victim_offenders      BIGINT
);
CREATE TABLE IF NOT EXISTS alerts (
    source_dataset      TEXT,
    district            TEXT,
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    event_count         BIGINT,
    window_first_event  TIMESTAMP,
    window_last_event   TIMESTAMP,
    threshold           INTEGER,
    detected_at         TIMESTAMP,
    alert_severity      TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_source ON alerts (source_dataset);
CREATE TABLE IF NOT EXISTS events (
    kafka_timestamp     TIMESTAMP,
    event_id            TEXT,
    source_dataset      TEXT,
    victim_id           TEXT,
    incident_date       TEXT,
    incident_time       TEXT,
    location            TEXT,
    district            TEXT,
    beat                TEXT,
    crime_type          TEXT,
    injury_type         TEXT,
    event_label         TEXT,
    severity            INTEGER,
    latitude            TEXT,
    longitude           TEXT,
    is_arrest           BOOLEAN,
    processed_timestamp BIGINT,
    processed_at        TIMESTAMP,
    received_lag_ms     BIGINT,
    is_alert            BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_events_processed_at   ON events (processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_severity       ON events (severity);
CREATE INDEX IF NOT EXISTS idx_events_district       ON events (district);
CREATE INDEX IF NOT EXISTS idx_events_source_dataset ON events (source_dataset);
CREATE INDEX IF NOT EXISTS idx_events_crime_type     ON events (crime_type);`

	if _, err := db.ExecContext(ctx, ddl); err != nil {
		_ = db.Close()
		return fmt.Errorf("ddl: %w", err)
	}

	pgDB = db
	return nil
}

// initMongo connects to MongoDB and pins the alerts collection.
func initMongo(connStr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return fmt.Errorf("ping: %w", err)
	}

	mongoClient = client
	mongoColl = client.Database("whitechristmas").Collection("alerts")
	return nil
}

// ─── REST handlers ────────────────────────────────────────────────────────────

// EventHistoryHandler serves GET /events/history?limit=50&offset=0&dataset=crimes
func EventHistoryHandler() http.HandlerFunc {
	type row struct {
		EventID            string  `json:"event_id"`
		SourceDataset      *string `json:"source_dataset"`
		VictimID           *string `json:"victim_id"`
		IncidentDate       string  `json:"incident_date"`
		IncidentTime       *string `json:"incident_time"`
		Location           *string `json:"location"`
		District           *string `json:"district"`
		Beat               *string `json:"beat"`
		CrimeType          *string `json:"crime_type"`
		InjuryType         *string `json:"injury_type"`
		EventLabel         *string `json:"event_label"`
		Severity           *int    `json:"severity"`
		Latitude           *string `json:"latitude"`
		Longitude          *string `json:"longitude"`
		IsArrest           *bool   `json:"is_arrest"`
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
		dataset := r.URL.Query().Get("dataset")

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		var queryRows *sql.Rows
		var err error
		if dataset != "" {
			queryRows, err = pgDB.QueryContext(ctx, `
				SELECT event_id, source_dataset, victim_id, incident_date, incident_time,
				       location, district, beat, crime_type, injury_type, event_label,
				       severity, latitude, longitude, is_arrest, processed_timestamp,
				       processed_at, is_alert
				FROM events
				WHERE source_dataset = $1
				ORDER BY processed_at DESC
				LIMIT $2 OFFSET $3`, dataset, limit, offset)
		} else {
			queryRows, err = pgDB.QueryContext(ctx, `
				SELECT event_id, source_dataset, victim_id, incident_date, incident_time,
				       location, district, beat, crime_type, injury_type, event_label,
				       severity, latitude, longitude, is_arrest, processed_timestamp,
				       processed_at, is_alert
				FROM events
				ORDER BY processed_at DESC
				LIMIT $1 OFFSET $2`, limit, offset)
		}
		if err != nil {
			log.Printf("events/history query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = queryRows.Close() }()

		var events []row
		for queryRows.Next() {
			var e row
			var processedAt time.Time
			if err := queryRows.Scan(
				&e.EventID, &e.SourceDataset, &e.VictimID, &e.IncidentDate, &e.IncidentTime,
				&e.Location, &e.District, &e.Beat, &e.CrimeType, &e.InjuryType, &e.EventLabel,
				&e.Severity, &e.Latitude, &e.Longitude, &e.IsArrest,
				&e.ProcessedTimestamp, &processedAt, &e.IsAlert,
			); err != nil {
				continue
			}
			e.ProcessedAt = processedAt.Format(time.RFC3339)
			events = append(events, e)
		}
		if events == nil {
			events = []row{}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"events": events, "limit": limit, "offset": offset})
	}
}

// DistrictSummaryHandler serves GET /districts/summary
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
				district,
				COUNT(*) AS total_events,
				COALESCE(AVG(severity), 0) AS avg_severity,
				SUM(CASE WHEN severity >= 4 THEN 1 ELSE 0 END) AS critical_count,
				MAX(processed_at) AS last_seen
			FROM events
			WHERE district IS NOT NULL
			GROUP BY district
			ORDER BY total_events DESC`)
		if err != nil {
			log.Printf("districts/summary query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = rows.Close() }()

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
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"districts": districts})
	}
}

// TrendsHandler serves GET /analytics/trends?hours=24
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
			WHERE processed_at > NOW() - $1 * INTERVAL '1 hour'
			GROUP BY hour
			ORDER BY hour ASC`, hours)
		if err != nil {
			log.Printf("analytics/trends query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = rows.Close() }()

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
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"trends": trends, "hours": hours})
	}
}

// DatasetSummaryHandler serves GET /datasets/summary
// Returns per-dataset event counts, avg severity, and critical event counts.
func DatasetSummaryHandler() http.HandlerFunc {
	type datasetRow struct {
		Dataset       string  `json:"dataset"`
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
				COALESCE(source_dataset, 'unknown') AS dataset,
				COUNT(*) AS total_events,
				COALESCE(AVG(severity), 0) AS avg_severity,
				SUM(CASE WHEN severity >= 4 THEN 1 ELSE 0 END) AS critical_count,
				MAX(processed_at) AS last_seen
			FROM events
			GROUP BY source_dataset
			ORDER BY total_events DESC`)
		if err != nil {
			log.Printf("datasets/summary query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = rows.Close() }()

		var datasets []datasetRow
		for rows.Next() {
			var d datasetRow
			var lastSeen time.Time
			if err := rows.Scan(&d.Dataset, &d.TotalEvents, &d.AvgSeverity, &d.CriticalCount, &lastSeen); err != nil {
				continue
			}
			d.LastSeen = lastSeen.Format(time.RFC3339)
			datasets = append(datasets, d)
		}
		if datasets == nil {
			datasets = []datasetRow{}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"datasets": datasets})
	}
}

// AlertsRecentHandler serves GET /alerts/recent?limit=20&dataset=crimes
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
		dataset := r.URL.Query().Get("dataset")

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		filter := bson.D{}
		if dataset != "" {
			filter = bson.D{{Key: "source_dataset", Value: dataset}}
		}

		opts := options.Find().
			SetSort(bson.D{{Key: "processed_at", Value: -1}}).
			SetLimit(limit)

		cursor, err := mongoColl.Find(ctx, filter, opts)
		if err != nil {
			log.Printf("alerts/recent query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = cursor.Close(ctx) }()

		var alerts []bson.M
		if err := cursor.All(ctx, &alerts); err != nil {
			http.Error(w, `{"error":"decode failed"}`, http.StatusInternalServerError)
			return
		}
		if alerts == nil {
			alerts = []bson.M{}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"alerts": alerts})
	}
}

// tableNotFound returns true when a query fails because the table doesn't exist yet.
func tableNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "does not exist")
}

// CrimeTrendsHandler serves GET /analytics/crime-trends?year=YYYY
// Without year param: returns all-time monthly totals (12 rows, one per month).
// With year param: returns monthly totals for that specific year only.
func CrimeTrendsHandler() http.HandlerFunc {
	type row struct {
		Month      int   `json:"month"`
		CrimeCount int64 `json:"crime_count"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if pgDB == nil {
			http.Error(w, `{"error":"PostgreSQL not connected"}`, http.StatusServiceUnavailable)
			return
		}
		year := r.URL.Query().Get("year")
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()

		var qrows *sql.Rows
		var err error
		if year != "" {
			qrows, err = pgDB.QueryContext(ctx,
				`SELECT month, SUM(crime_count) AS crime_count
				 FROM crime_trends WHERE year = $1
				 GROUP BY month ORDER BY month`, year)
		} else {
			// Aggregate across all years so a partial current year
			// does not truncate the month axis.
			qrows, err = pgDB.QueryContext(ctx,
				`SELECT month, SUM(crime_count) AS crime_count
				 FROM crime_trends
				 GROUP BY month ORDER BY month`)
		}
		if tableNotFound(err) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": []struct{}{}, "status": "batch job not yet run"})
			return
		}
		if err != nil {
			log.Printf("analytics/crime-trends query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = qrows.Close() }()
		var results []row
		for qrows.Next() {
			var rw row
			if err := qrows.Scan(&rw.Month, &rw.CrimeCount); err != nil {
				continue
			}
			results = append(results, rw)
		}
		if results == nil {
			results = []row{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": results})
	}
}

// HotspotsHandler serves GET /analytics/hotspots
func HotspotsHandler() http.HandlerFunc {
	type row struct {
		ClusterID    int     `json:"cluster_id"`
		CrimeCount   int64   `json:"crime_count"`
		AvgLat       float64 `json:"avg_lat"`
		AvgLon       float64 `json:"avg_lon"`
		PrimaryTypes string  `json:"primary_types"`
		CentroidLat  float64 `json:"centroid_lat"`
		CentroidLon  float64 `json:"centroid_lon"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if pgDB == nil {
			http.Error(w, `{"error":"PostgreSQL not connected"}`, http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		qrows, err := pgDB.QueryContext(ctx,
			`SELECT cluster_id, crime_count, avg_lat, avg_lon,
			        COALESCE(primary_types,''), centroid_lat, centroid_lon
			 FROM hotspots ORDER BY crime_count DESC`)
		if tableNotFound(err) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": []struct{}{}, "status": "batch job not yet run"})
			return
		}
		if err != nil {
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = qrows.Close() }()
		var results []row
		for qrows.Next() {
			var rw row
			if err := qrows.Scan(&rw.ClusterID, &rw.CrimeCount, &rw.AvgLat, &rw.AvgLon,
				&rw.PrimaryTypes, &rw.CentroidLat, &rw.CentroidLon); err != nil {
				continue
			}
			results = append(results, rw)
		}
		if results == nil {
			results = []row{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": results})
	}
}

// CorrelationsHandler serves GET /analytics/correlations?type=district|community
func CorrelationsHandler() http.HandlerFunc {
	type row struct {
		CorrelationType   string   `json:"correlation_type"`
		GroupKey          *string  `json:"group_key"`
		CommunityArea     *string  `json:"community_area"`
		TotalCrimes       *int64   `json:"total_crimes"`
		MetricA           *float64 `json:"metric_a"`
		MetricB           *float64 `json:"metric_b"`
		ViolenceIncidents *int64   `json:"violence_incidents"`
		SexOffenderCount  *int64   `json:"sex_offender_count"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if pgDB == nil {
			http.Error(w, `{"error":"PostgreSQL not connected"}`, http.StatusServiceUnavailable)
			return
		}
		corrType := r.URL.Query().Get("type")
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		var whereClause string
		switch corrType {
		case "community":
			whereClause = `WHERE correlation_type = 'sex_offender_density_vs_crime_rate_by_community'`
		default:
			whereClause = `WHERE correlation_type = 'violence_rate_vs_arrest_rate_by_district'`
		}

		qrows, err := pgDB.QueryContext(ctx, fmt.Sprintf(
			`SELECT correlation_type, group_key, community_area, total_crimes,
			        metric_a, metric_b, violence_incidents, sex_offender_count
			 FROM correlations %s ORDER BY total_crimes DESC NULLS LAST LIMIT 25`, whereClause))
		if tableNotFound(err) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": []struct{}{}, "status": "batch job not yet run"})
			return
		}
		if err != nil {
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = qrows.Close() }()
		var results []row
		for qrows.Next() {
			var rw row
			if err := qrows.Scan(&rw.CorrelationType, &rw.GroupKey, &rw.CommunityArea,
				&rw.TotalCrimes, &rw.MetricA, &rw.MetricB, &rw.ViolenceIncidents, &rw.SexOffenderCount); err != nil {
				continue
			}
			results = append(results, rw)
		}
		if results == nil {
			results = []row{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": results})
	}
}

// ArrestRatesHandler serves GET /analytics/arrest-rates
func ArrestRatesHandler() http.HandlerFunc {
	type row struct {
		PrimaryType  string  `json:"primary_type"`
		TotalCrimes  int64   `json:"total_crimes"`
		TotalArrests int64   `json:"total_arrests"`
		ArrestRate   float64 `json:"arrest_rate"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if pgDB == nil {
			http.Error(w, `{"error":"PostgreSQL not connected"}`, http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		qrows, err := pgDB.QueryContext(ctx,
			`SELECT primary_type, total_crimes, total_arrests, arrest_rate
			 FROM top_arrest_rate_crime_types ORDER BY arrest_rate DESC`)
		if tableNotFound(err) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": []struct{}{}, "status": "batch job not yet run"})
			return
		}
		if err != nil {
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = qrows.Close() }()
		var results []row
		for qrows.Next() {
			var rw row
			if err := qrows.Scan(&rw.PrimaryType, &rw.TotalCrimes, &rw.TotalArrests, &rw.ArrestRate); err != nil {
				continue
			}
			results = append(results, rw)
		}
		if results == nil {
			results = []row{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": results})
	}
}

// ViolenceHandler serves GET /analytics/violence
func ViolenceHandler() http.HandlerFunc {
	type row struct {
		Month             int     `json:"month"`
		District          string  `json:"district"`
		Homicides         int64   `json:"homicides"`
		NonFatalShootings int64   `json:"non_fatal_shootings"`
		GunShotIncidents  int64   `json:"gunshot_incidents"`
		TotalIncidents    int64   `json:"total_incidents"`
		GunShotProportion float64 `json:"gunshot_proportion_overall"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if pgDB == nil {
			http.Error(w, `{"error":"PostgreSQL not connected"}`, http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		qrows, err := pgDB.QueryContext(ctx,
			`SELECT month, COALESCE(district,'?'), homicides, non_fatal_shootings,
			        gunshot_incidents, total_incidents, gunshot_proportion_overall
			 FROM violence_analysis ORDER BY month, district`)
		if tableNotFound(err) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": []struct{}{}, "status": "batch job not yet run"})
			return
		}
		if err != nil {
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = qrows.Close() }()
		var results []row
		for qrows.Next() {
			var rw row
			if err := qrows.Scan(&rw.Month, &rw.District, &rw.Homicides, &rw.NonFatalShootings,
				&rw.GunShotIncidents, &rw.TotalIncidents, &rw.GunShotProportion); err != nil {
				continue
			}
			results = append(results, rw)
		}
		if results == nil {
			results = []row{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": results})
	}
}

// SexOffendersHandler serves GET /analytics/sex-offenders
// Returns a priority/race breakdown from sex_offender_proximity.
func SexOffendersHandler() http.HandlerFunc {
	type raceRow struct {
		Race  string `json:"race"`
		Count int64  `json:"count"`
	}
	type response struct {
		TotalOffenders   int64     `json:"total_offenders"`
		MinorVictimCount int64     `json:"minor_victim_count"`
		PriorityCount    int64     `json:"priority_count"`
		StandardCount    int64     `json:"standard_count"`
		ByRace           []raceRow `json:"by_race"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if pgDB == nil {
			http.Error(w, `{"error":"PostgreSQL not connected"}`, http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		var resp response
		err := pgDB.QueryRowContext(ctx, `
			SELECT
			  COUNT(*) AS total_offenders,
			  COUNT(*) FILTER (WHERE is_victim_minor = true) AS minor_victim_count,
			  COUNT(*) FILTER (WHERE priority_flag = 'PRIORITY') AS priority_count,
			  COUNT(*) FILTER (WHERE priority_flag = 'STANDARD') AS standard_count
			FROM sex_offender_proximity`).
			Scan(&resp.TotalOffenders, &resp.MinorVictimCount, &resp.PriorityCount, &resp.StandardCount)
		if tableNotFound(err) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": response{}, "status": "batch job not yet run"})
			return
		}
		if err != nil {
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}

		rows, err := pgDB.QueryContext(ctx, `
			SELECT COALESCE(race,'UNKNOWN') AS race, COUNT(*) AS count
			FROM sex_offender_proximity
			WHERE race IS NOT NULL AND race <> ''
			GROUP BY race ORDER BY count DESC LIMIT 10`)
		if err == nil {
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var rr raceRow
				if err := rows.Scan(&rr.Race, &rr.Count); err == nil {
					resp.ByRace = append(resp.ByRace, rr)
				}
			}
		}
		if resp.ByRace == nil {
			resp.ByRace = []raceRow{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": resp})
	}
}

// SeverityHandler serves GET /analytics/severity
// Returns cumulative event counts grouped by severity level from the events table.
func SeverityHandler() http.HandlerFunc {
	type severityRow struct {
		Severity int   `json:"severity"`
		Count    int64 `json:"count"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if pgDB == nil {
			http.Error(w, `{"error":"PostgreSQL not connected"}`, http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		rows, err := pgDB.QueryContext(ctx, `
			SELECT COALESCE(severity, 1) AS severity, COUNT(*) AS count
			FROM events
			GROUP BY severity
			ORDER BY severity`)
		if err != nil {
			log.Printf("analytics/severity query: %v", err)
			http.Error(w, `{"error":"query failed"}`, http.StatusInternalServerError)
			return
		}
		defer func() { _ = rows.Close() }()
		var results []severityRow
		for rows.Next() {
			var sr severityRow
			if err := rows.Scan(&sr.Severity, &sr.Count); err != nil {
				continue
			}
			results = append(results, sr)
		}
		if results == nil {
			results = []severityRow{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": results})
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

	// ALERTS_TOPICS is a comma-separated list of per-dataset alert topics.
	// Each topic gets its own consumer goroutine and its own consumer group.
	alertsTopicsStr := os.Getenv("ALERTS_TOPICS")
	if alertsTopicsStr == "" {
		alertsTopicsStr = "alerts-crimes,alerts-violence,alerts-arrests,alerts-sex-offenders"
	}
	alertsTopics := make([]string, 0)
	for _, t := range strings.Split(alertsTopicsStr, ",") {
		if t = strings.TrimSpace(t); t != "" {
			alertsTopics = append(alertsTopics, t)
		}
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

	fmt.Printf("📊 Kafka Brokers:   %s\n", kafkaBrokers)
	fmt.Printf("📖 Alert Topics:    %s\n", strings.Join(alertsTopics, ", "))
	fmt.Printf("🌐 API Port:        %s\n", port)
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

	// Start one Kafka consumer goroutine per alert topic.
	// Each consumer has its own group ID so they don't compete for the same partitions.
	for _, topic := range alertsTopics {
		go KafkaConsumer(hub, kafkaBrokers, topic)
	}

	// Setup router
	r := chi.NewRouter()
	r.Use(corsMiddleware)
	r.Use(instrumentMiddleware)

	// Routes
	r.Get("/health", HealthHandler())
	r.Get("/stats", StatsHandler(hub))
	r.HandleFunc("/ws", WebSocketHandler(hub))
	r.Handle("/metrics", promhttp.Handler())

	// Historical / analytics REST endpoints (backed by PostgreSQL + MongoDB)
	r.Get("/events/history", EventHistoryHandler())
	r.Get("/districts/summary", DistrictSummaryHandler())
	r.Get("/datasets/summary", DatasetSummaryHandler())
	r.Get("/analytics/trends", TrendsHandler())
	r.Get("/alerts/recent", AlertsRecentHandler())

	// Batch analytics endpoints (populated by BatchAnalytics Spark job)
	r.Get("/analytics/crime-trends", CrimeTrendsHandler())
	r.Get("/analytics/hotspots", HotspotsHandler())
	r.Get("/analytics/correlations", CorrelationsHandler())
	r.Get("/analytics/arrest-rates", ArrestRatesHandler())
	r.Get("/analytics/violence", ViolenceHandler())
	r.Get("/analytics/sex-offenders", SexOffendersHandler())
	r.Get("/analytics/severity", SeverityHandler())

	// Create server
	srv := &http.Server{
		Addr:        ":" + port,
		Handler:     r,
		ReadTimeout: 15 * time.Second,
		// WriteTimeout intentionally omitted — WebSocket connections are long-lived
		// and a write deadline would kill them after 15 s.
		IdleTimeout: 60 * time.Second,
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
		fmt.Printf("  Prometheus:       http://localhost:%s/metrics\n", port)
		fmt.Printf("  Event history:    http://localhost:%s/events/history[?dataset=crimes]\n", port)
		fmt.Printf("  Dataset summary:  http://localhost:%s/datasets/summary\n", port)
		fmt.Printf("  District summary: http://localhost:%s/districts/summary\n", port)
		fmt.Printf("  Hourly trends:    http://localhost:%s/analytics/trends\n", port)
		fmt.Printf("  Recent alerts:    http://localhost:%s/alerts/recent[?dataset=crimes]\n", port)
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
		_ = pgDB.Close()
	}
	if mongoClient != nil {
		_ = mongoClient.Disconnect(ctx)
	}

	fmt.Println("✅ Server stopped")
}
