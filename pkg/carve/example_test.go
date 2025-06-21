package carve_test

import (
	"fmt"
	"regexp"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"carve/pkg/carve"
)

// Example demonstrates basic usage of the carve library
func Example() {
	// Define a regex pattern with named capture groups
	pattern := `^(?P<timestamp>[^ ]+) (?P<level>\w+) (?P<message>.+)`
	re := regexp.MustCompile(pattern)

	// Extract schema from the pattern
	schema, err := carve.ExtractSchema(re)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Schema has %d fields\n", len(schema.Fields()))
	for i, field := range schema.Fields() {
		fmt.Printf("Field %d: %s (%s)\n", i, field.Name, field.Type)
	}

	// Output:
	// Schema has 3 fields
	// Field 0: timestamp (utf8)
	// Field 1: level (utf8)
	// Field 2: message (utf8)
}

// ExampleExtractSchema demonstrates schema extraction from regex patterns
func ExampleExtractSchema() {
	// Simple pattern with named groups
	pattern := `^(?P<level>\w+) (?P<message>.+)`
	re := regexp.MustCompile(pattern)

	schema, err := carve.ExtractSchema(re)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Fields: %d\n", len(schema.Fields()))
	for _, field := range schema.Fields() {
		fmt.Printf("- %s (%s)\n", field.Name, field.Type)
	}

	// Output:
	// Fields: 2
	// - level (utf8)
	// - message (utf8)
}

// ExampleParseLine demonstrates parsing individual log lines
func ExampleParseLine() {
	pattern := `^(?P<timestamp>\d{4}-[^ ]+) (?P<level>\w+) (?P<message>.+)`
	re := regexp.MustCompile(pattern)

	// Test with valid and invalid lines
	lines := []string{
		"2023-01-01T10:00:00.123Z INFO Application started",
		"invalid log line",
		"2023-01-01T10:00:01.456Z ERROR Database failed",
	}

	for i, line := range lines {
		values := carve.ParseLine(line, re)
		if values != nil {
			fmt.Printf("Line %d: %d fields extracted\n", i+1, len(values))
		} else {
			fmt.Printf("Line %d: no match\n", i+1)
		}
	}

	// Output:
	// Line 1: 3 fields extracted
	// Line 2: no match
	// Line 3: 3 fields extracted
}

// ExampleNewArrowWriter demonstrates batching with ArrowWriter
func ExampleNewArrowWriter() {
	pattern := `^(?P<level>\w+) (?P<message>.+)`
	re := regexp.MustCompile(pattern)
	schema, _ := carve.ExtractSchema(re)

	// Create writer with small batch size for demonstration
	writer := carve.NewArrowWriter(schema, memory.DefaultAllocator, 2)

	// Process some lines
	lines := []string{
		"INFO Starting application",
		"WARN Configuration missing",
		"ERROR Database failed",
	}

	batchCount := 0
	for _, line := range lines {
		if values := carve.ParseLine(line, re); values != nil {
			writer.Append(values)

			if writer.ShouldFlush() {
				record := writer.Flush()
				batchCount++
				fmt.Printf("Batch %d: %d rows\n", batchCount, record.NumRows())
				record.Release()
			}
		}
	}

	// Final flush
	if writer.Rows() > 0 {
		record := writer.Flush()
		fmt.Printf("Final batch: %d rows\n", record.NumRows())
		record.Release()
	}

	// Output:
	// Batch 1: 2 rows
	// Final batch: 1 rows
}

// Example_webServerLogs demonstrates parsing Apache/Nginx access logs
func Example_webServerLogs() {
	// Common Log Format pattern
	pattern := `^(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>\w+) (?P<path>\S+) (?P<protocol>[^"]+)" (?P<status>\d+) (?P<size>\d+)`
	re := regexp.MustCompile(pattern)

	logLine := `192.168.1.1 - - [01/Jan/2023:10:00:00 +0000] "GET /api/users HTTP/1.1" 200 1234`

	values := carve.ParseLine(logLine, re)
	if values != nil {
		schema, _ := carve.ExtractSchema(re)
		fmt.Printf("Parsed %d fields:\n", len(values))
		for i, field := range schema.Fields() {
			fmt.Printf("%s: %s\n", field.Name, values[i])
		}
	}

	// Output:
	// Parsed 7 fields:
	// ip: 192.168.1.1
	// timestamp: 01/Jan/2023:10:00:00 +0000
	// method: GET
	// path: /api/users
	// protocol: HTTP/1.1
	// status: 200
	// size: 1234
}

// Example_structuredLogs demonstrates parsing structured application logs
func Example_structuredLogs() {
	pattern := `^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) \[(?P<thread>[^\]]+)\] (?P<level>\w+) (?P<logger>\S+) - (?P<message>.+)`
	re := regexp.MustCompile(pattern)

	logLine := `2023-01-01T10:00:00.123Z [main] INFO com.example.App - Application started`

	values := carve.ParseLine(logLine, re)
	if values != nil {
		fmt.Printf("Timestamp: %s\n", values[0])
		fmt.Printf("Thread: %s\n", values[1])
		fmt.Printf("Level: %s\n", values[2])
		fmt.Printf("Logger: %s\n", values[3])
		fmt.Printf("Message: %s\n", values[4])
	}

	// Output:
	// Timestamp: 2023-01-01T10:00:00.123Z
	// Thread: main
	// Level: INFO
	// Logger: com.example.App
	// Message: Application started
}

// Example_errorHandling demonstrates graceful error handling
func Example_errorHandling() {
	pattern := `^(?P<timestamp>\d{4}-\d{2}-\d{2}) (?P<level>\w+) (?P<message>.+)`
	re := regexp.MustCompile(pattern)

	lines := []string{
		"2023-01-01 INFO Valid log line",
		"invalid line format",
		"2023-01-02 ERROR Another valid line",
		"",
	}

	validCount := 0
	invalidCount := 0

	for _, line := range lines {
		values := carve.ParseLine(line, re)
		if values != nil {
			validCount++
		} else {
			invalidCount++
		}
	}

	fmt.Printf("Valid: %d, Invalid: %d\n", validCount, invalidCount)

	// Output:
	// Valid: 2, Invalid: 2
}

// Example_memoryManagement demonstrates proper Arrow memory handling
func Example_memoryManagement() {
	pattern := `^(?P<level>\w+) (?P<message>.+)`
	re := regexp.MustCompile(pattern)
	schema, _ := carve.ExtractSchema(re)

	// Use custom allocator
	allocator := memory.NewGoAllocator()
	writer := carve.NewArrowWriter(schema, allocator, 3)

	lines := []string{
		"INFO Starting",
		"WARN Warning",
		"ERROR Failed",
		"INFO Completed",
	}

	var records []arrow.Record
	for _, line := range lines {
		if values := carve.ParseLine(line, re); values != nil {
			writer.Append(values)

			if writer.ShouldFlush() {
				record := writer.Flush()
				records = append(records, record)
				fmt.Printf("Created record with %d rows\n", record.NumRows())
			}
		}
	}

	// Final flush
	if writer.Rows() > 0 {
		record := writer.Flush()
		records = append(records, record)
		fmt.Printf("Final record with %d rows\n", record.NumRows())
	}

	// Important: Release all records
	for i, record := range records {
		record.Release()
		fmt.Printf("Released record %d\n", i+1)
	}

	// Output:
	// Created record with 3 rows
	// Final record with 1 rows
	// Released record 1
	// Released record 2
}
