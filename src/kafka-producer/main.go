package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// CrimeEvent represents a structured crime event
type CrimeEvent struct {
	EventID              string    `json:"event_id"`
	VictimID             *string   `json:"victim_id"`
	IncidentDate         string    `json:"incident_date"`
	IncidentTime         *string   `json:"incident_time"`
	Location             *string   `json:"location"`
	District             *string   `json:"district"`
	InjuryType           *string   `json:"injury_type"`
	Severity             *int      `json:"severity"`
	ProcessedTimestamp   int64     `json:"processed_timestamp"`
}

// CrimeRecord represents a row from the CSV
type CrimeRecord struct {
	Date                  string
	Time                  string
	VictimsName          string
	BodyPartInjured      string
	District             string
	CommunityArea        string
	LocationDescription  string
	Latitude             string
	Longitude            string
}

// CalculateSeverity determines severity level from injury type
func CalculateSeverity(injuryType *string) *int {
	if injuryType == nil || *injuryType == "" {
		severity := 1
		return &severity
	}

	lower := *injuryType
	var severity int

	switch {
	case contains(lower, "fatality") || contains(lower, "fatal") || contains(lower, "death"):
		severity = 5
	case contains(lower, "homicide"):
		severity = 5
	case contains(lower, "shooting") || contains(lower, "gunshot"):
		severity = 4
	case contains(lower, "head") || contains(lower, "brain"):
		severity = 4
	case contains(lower, "chest") || contains(lower, "torso"):
		severity = 3
	case contains(lower, "leg") || contains(lower, "arm"):
		severity = 2
	case contains(lower, "other"):
		severity = 1
	default:
		severity = 2
	}

	return &severity
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// StringPtr returns a pointer to a string
func StringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// CrimeRecordToEvent converts a CSV record to a CrimeEvent
func CrimeRecordToEvent(record CrimeRecord) CrimeEvent {
	severity := CalculateSeverity(StringPtr(record.BodyPartInjured))
	
	return CrimeEvent{
		EventID:            uuid.New().String(),
		VictimID:           StringPtr(record.VictimsName),
		IncidentDate:       record.Date,
		IncidentTime:       StringPtr(record.Time),
		Location:           StringPtr(record.LocationDescription),
		District:           StringPtr(record.District),
		InjuryType:         StringPtr(record.BodyPartInjured),
		Severity:           severity,
		ProcessedTimestamp: time.Now().UnixMilli(),
	}
}

func main() {
	// Parse command line flags
	csvPath := flag.String("csv", "/data/Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings_20260505.csv", 
		"Path to CSV data file")
	kafkaBrokers := flag.String("brokers", "localhost:9092", "Kafka bootstrap servers")
	topic := flag.String("topic", "raw-events", "Kafka topic to publish to")
	eventsPerSec := flag.Int("rate", 10, "Events per second rate limit")
	maxEvents := flag.Int("max", 0, "Maximum events to publish (0 = all)")

	flag.Parse()

	// Resolve CSV path relative to data directory if needed
	if !filepath.IsAbs(*csvPath) {
		*csvPath = filepath.Join("/data", *csvPath)
	}

	fmt.Println("🚀 WhiteChristmas Kafka Producer (Go)")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("CSV Path:        %s\n", *csvPath)
	fmt.Printf("Kafka Brokers:   %s\n", *kafkaBrokers)
	fmt.Printf("Topic:           %s\n", *topic)
	fmt.Printf("Rate Limit:      %d/sec\n", *eventsPerSec)
	if *maxEvents > 0 {
		fmt.Printf("Max Events:      %d\n", *maxEvents)
	}
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	// Verify CSV file exists
	if _, err := os.Stat(*csvPath); err != nil {
		log.Fatalf("❌ CSV file not found: %s", *csvPath)
	}

	// Create Kafka writer
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{*kafkaBrokers},
		Topic:        *topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	})
	defer func() {
		if err := w.Close(); err != nil {
			log.Fatalf("failed to close writer: %v", err)
		}
	}()

	fmt.Println("✓ Kafka producer initialized")
	fmt.Println()

	// Open CSV file
	file, err := os.Open(*csvPath)
	if err != nil {
		log.Fatalf("❌ Failed to open CSV file: %v", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(bufio.NewReader(file))

	// Read header
	headers, err := reader.Read()
	if err != nil {
		log.Fatalf("❌ Failed to read CSV headers: %v", err)
	}

	// Find column indices
	var dateIdx, timeIdx, victimIdx, injuryIdx, districtIdx, locIdx int
	for i, header := range headers {
		switch header {
		case "Date":
			dateIdx = i
		case "Time":
			timeIdx = i
		case "Victim's Name":
			victimIdx = i
		case "Body Part Injured":
			injuryIdx = i
		case "District":
			districtIdx = i
		case "Location Description":
			locIdx = i
		}
	}

	// Stream CSV records to Kafka
	eventCount := 0
	publishedCount := 0
	rateLimiter := time.Tick(time.Duration(1000/int64(*eventsPerSec)) * time.Millisecond)

	fmt.Println("📖 Reading CSV and streaming to Kafka...")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("Error reading CSV: %v", err)
			continue
		}

		if *maxEvents > 0 && eventCount >= *maxEvents {
			fmt.Println("✓ Reached max event limit")
			break
		}

		eventCount++

		// Extract fields
		var date, time_, victim, injury, district, location string
		if dateIdx < len(record) {
			date = record[dateIdx]
		}
		if timeIdx < len(record) {
			time_ = record[timeIdx]
		}
		if victimIdx < len(record) {
			victim = record[victimIdx]
		}
		if injuryIdx < len(record) {
			injury = record[injuryIdx]
		}
		if districtIdx < len(record) {
			district = record[districtIdx]
		}
		if locIdx < len(record) {
			location = record[locIdx]
		}

		// Convert to crime event
		crimeRecord := CrimeRecord{
			Date:                 date,
			Time:                 time_,
			VictimsName:         victim,
			BodyPartInjured:      injury,
			District:             district,
			LocationDescription:  location,
		}

		event := CrimeRecordToEvent(crimeRecord)

		// Serialize to JSON
		payload, err := json.Marshal(event)
		if err != nil {
			log.Printf("Failed to marshal event: %v", err)
			continue
		}

		// Send to Kafka
		err = w.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(event.EventID),
			Value: payload,
		})

		if err != nil {
			log.Printf("Failed to write message: %v", err)
			continue
		}

		publishedCount++
		if publishedCount%100 == 0 || publishedCount < 10 {
			fmt.Printf("📨 Published event #%d: %s\n", publishedCount, event.EventID)
		}

		if eventCount%100 == 0 {
			fmt.Printf("  → Processing event #%d, published: #%d\n", eventCount, publishedCount)
		}

		// Rate limiting
		<-rateLimiter
	}

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("✅ Publishing complete!")
	fmt.Printf("   Total records read:       %d\n", eventCount)
	fmt.Printf("   Successfully published:  %d\n", publishedCount)
	fmt.Println()
}
