package carve

import (
	"regexp"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkSmallFile tests performance on small files with high match rate
func BenchmarkSmallFile(b *testing.B) {
	re := regexp.MustCompile(`^(?P<ts>[^ ]+) (?P<level>\w+) (?P<msg>.+)`)
	schema, _ := ExtractSchema(re)

	// Generate small test data (100 lines, 95% match rate)
	lines := generateLogLines(100, 0.95)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 1000)
		processLines(lines, re, writer)
	}
}

// BenchmarkMediumFile tests performance on medium files with mixed match rate
func BenchmarkMediumFile(b *testing.B) {
	re := regexp.MustCompile(`^(?P<ts>[^ ]+) (?P<level>\w+) (?P<msg>.+)`)
	schema, _ := ExtractSchema(re)

	// Generate medium test data (1000 lines, 75% match rate)
	lines := generateLogLines(1000, 0.75)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 5000)
		processLines(lines, re, writer)
	}
}

// BenchmarkLargeFile tests performance on large files
func BenchmarkLargeFile(b *testing.B) {
	re := regexp.MustCompile(`^(?P<ts>[^ ]+) (?P<level>\w+) (?P<msg>.+)`)
	schema, _ := ExtractSchema(re)

	// Generate large test data (10000 lines, 80% match rate)
	lines := generateLogLines(10000, 0.80)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 10000)
		processLines(lines, re, writer)
	}
}

// BenchmarkPathologicalInput tests performance on difficult input
func BenchmarkPathologicalInput(b *testing.B) {
	re := regexp.MustCompile(`^(?P<ts>[^ ]+) (?P<level>\w+) (?P<msg>.+)`)
	schema, _ := ExtractSchema(re)

	// Generate pathological data (low match rate, empty lines)
	lines := generatePathologicalLines(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 1000)
		processLines(lines, re, writer)
	}
}

// BenchmarkComplexPattern tests performance with complex regex patterns
func BenchmarkComplexPattern(b *testing.B) {
	re := regexp.MustCompile(`^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) \[(?P<thread>[^\]]+)\] (?P<level>\w+) +(?P<logger>[^ ]+) - (?P<message>.+)`)
	schema, _ := ExtractSchema(re)

	// Generate complex log format
	lines := generateComplexLogLines(1000, 0.90)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 1000)
		processLines(lines, re, writer)
	}
}

// BenchmarkFlushingOverhead tests the overhead of frequent flushing
func BenchmarkFlushingOverhead(b *testing.B) {
	re := regexp.MustCompile(`^(?P<ts>[^ ]+) (?P<level>\w+) (?P<msg>.+)`)
	schema, _ := ExtractSchema(re)

	lines := generateLogLines(1000, 0.95)

	b.Run("SmallBatches", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer := NewArrowWriter(schema, memory.DefaultAllocator, 10) // Frequent flushing
			processLines(lines, re, writer)
		}
	})

	b.Run("LargeBatches", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer := NewArrowWriter(schema, memory.DefaultAllocator, 10000) // Infrequent flushing
			processLines(lines, re, writer)
		}
	})
}

// BenchmarkSchemaExtraction tests schema extraction performance
func BenchmarkSchemaExtraction(b *testing.B) {
	patterns := []string{
		`^(?P<ts>[^ ]+) (?P<level>\w+) (?P<msg>.+)`,
		`^(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>\w+) (?P<path>\S+) (?P<protocol>[^"]+)" (?P<status>\d+) (?P<size>\d+)`,
		`^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) \[(?P<thread>[^\]]+)\] (?P<level>\w+) +(?P<logger>[^ ]+) - (?P<message>.+)`,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, pattern := range patterns {
			re := regexp.MustCompile(pattern)
			_, err := ExtractSchema(re)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkParseLine tests line parsing performance
func BenchmarkParseLine(b *testing.B) {
	re := regexp.MustCompile(`^(?P<ts>[^ ]+) (?P<level>\w+) (?P<msg>.+)`)
	line := "2023-01-01T10:00:00.123Z INFO This is a test log message"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = ParseLine(line, re)
	}
}

// BenchmarkParseLineComplex tests complex pattern parsing
func BenchmarkParseLineComplex(b *testing.B) {
	re := regexp.MustCompile(`^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) \[(?P<thread>[^\]]+)\] (?P<level>\w+) +(?P<logger>[^ ]+) - (?P<message>.+)`)
	line := "2023-01-01T10:00:00.123Z [main] INFO  com.example.Service - Processing request with ID 12345"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = ParseLine(line, re)
	}
}

// BenchmarkArrowWriterAppend tests the append operation performance
func BenchmarkArrowWriterAppend(b *testing.B) {
	re := regexp.MustCompile(`^(?P<level>\w+) (?P<message>.+)`)
	schema, _ := ExtractSchema(re)
	writer := NewArrowWriter(schema, memory.DefaultAllocator, 100000) // Large batch to avoid flushing

	values := []string{"INFO", "Test message"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		writer.Append(values)
	}
}

// BenchmarkArrowWriterFlush tests the flush operation performance
func BenchmarkArrowWriterFlush(b *testing.B) {
	re := regexp.MustCompile(`^(?P<level>\w+) (?P<message>.+)`)
	schema, _ := ExtractSchema(re)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 1000)

		// Fill the writer with data
		values := []string{"INFO", "Test message"}
		for j := 0; j < 1000; j++ {
			writer.Append(values)
		}

		b.StartTimer()
		record := writer.Flush()
		record.Release()
	}
}

// BenchmarkMemoryAllocators compares different memory allocators
func BenchmarkMemoryAllocators(b *testing.B) {
	re := regexp.MustCompile(`^(?P<level>\w+) (?P<message>.+)`)
	schema, _ := ExtractSchema(re)
	lines := generateLogLines(1000, 0.95)

	b.Run("DefaultAllocator", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer := NewArrowWriter(schema, memory.DefaultAllocator, 1000)
			processLines(lines, re, writer)
		}
	})

	b.Run("GoAllocator", func(b *testing.B) {
		allocator := memory.NewGoAllocator()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer := NewArrowWriter(schema, allocator, 1000)
			processLines(lines, re, writer)
		}
	})
}

// Helper functions for benchmark data generation

func generateLogLines(count int, matchRate float64) []string {
	lines := make([]string, count)
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}

	for i := 0; i < count; i++ {
		if float64(i)/float64(count) < matchRate {
			level := levels[i%len(levels)]
			lines[i] = "2023-01-01T10:00:00.000Z " + level + " This is log message " + string(rune('A'+i%26))
		} else {
			// Non-matching lines
			lines[i] = "invalid log line " + string(rune('A'+i%26))
		}
	}

	return lines
}

func generateComplexLogLines(count int, matchRate float64) []string {
	lines := make([]string, count)
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	threads := []string{"main", "worker-1", "worker-2", "scheduler"}
	loggers := []string{"com.example.Service", "com.example.Controller", "com.example.Repository"}

	for i := 0; i < count; i++ {
		if float64(i)/float64(count) < matchRate {
			level := levels[i%len(levels)]
			thread := threads[i%len(threads)]
			logger := loggers[i%len(loggers)]
			lines[i] = "2023-01-01T10:00:00.123Z [" + thread + "] " + level + " " + logger + " - Processing request " + string(rune('A'+i%26))
		} else {
			lines[i] = "malformed log entry"
		}
	}

	return lines
}

func generatePathologicalLines(count int) []string {
	lines := make([]string, count)

	for i := 0; i < count; i++ {
		switch i % 10 {
		case 0, 1, 2: // 30% empty lines
			lines[i] = ""
		case 3, 4: // 20% very long lines
			lines[i] = "2023-01-01 INFO " + strings.Repeat("very long message ", 100)
		case 5, 6, 7: // 30% malformed lines
			lines[i] = "this does not match the pattern at all"
		default: // 20% valid lines
			lines[i] = "2023-01-01 INFO valid message"
		}
	}

	return lines
}

func processLines(lines []string, re *regexp.Regexp, writer *ArrowWriter) {
	for _, line := range lines {
		vals := ParseLine(line, re)
		if vals != nil {
			writer.Append(vals)
			if writer.ShouldFlush() {
				rec := writer.Flush()
				rec.Release()
			}
		}
	}

	// Final flush
	if writer.Rows() > 0 {
		rec := writer.Flush()
		rec.Release()
	}
}
