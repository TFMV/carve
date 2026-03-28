package carve

import (
	"regexp"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

const benchmarkPattern = `^(?P<ts>[^ ]+) (?P<level>\w+) (?P<msg>.+)`

// ============================================================================
// API Comparison Benchmarks: Regex vs Scanner
// ============================================================================

// BenchmarkAPIComparison_ParseLine tests the original regex-based ParseLine API
func BenchmarkAPIComparison_ParseLine(b *testing.B) {
	re := regexp.MustCompile(benchmarkPattern)
	lines := generateLogLinesAsBytes(1000, 0.95)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			_ = ExtractValues(string(line), re)
		}
	}
}

// BenchmarkAPIComparison_Scanner tests the new Scanner API with pre-compiled scan plan
func BenchmarkAPIComparison_Scanner(b *testing.B) {
	scanner, _ := NewExtractor(benchmarkPattern)
	lines := generateLogLinesAsBytes(1000, 0.95)
	out := make([][]byte, len(scanner.fields))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			scanner.Scan(line, out)
		}
	}
}

// BenchmarkAPIComparison_E2E_Regex tests end-to-end with regex-based API
func BenchmarkAPIComparison_E2E_Regex(b *testing.B) {
	re := regexp.MustCompile(benchmarkPattern)
	schema, _ := ExtractSchema(re)
	lines := generateLogLinesAsBytes(1000, 0.95)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 1000)
		for _, line := range lines {
			vals := ExtractValues(string(line), re)
			if vals != nil {
				writer.Append(vals)
			}
		}
		rec := writer.Flush()
		if rec != nil {
			rec.Release()
		}
	}
}

// BenchmarkAPIComparison_E2E_Scanner tests end-to-end with Scanner/Writer API
func BenchmarkAPIComparison_E2E_Scanner(b *testing.B) {
	ext, _ := NewExtractor(benchmarkPattern)
	scanner := ext.Scanner(Options{ZeroCopy: true})
	lines := generateLogLinesAsBytes(1000, 0.95)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewWriter(ext.Schema(), memory.DefaultAllocator, 1000)
		rec, _ := writer.WriteLinesSIMD(lines, scanner)
		if rec != nil {
			rec.Release()
		}
	}
}

// ============================================================================
// File Size Benchmarks
// ============================================================================

// BenchmarkSmallFile tests performance on small files with high match rate
func BenchmarkSmallFile(b *testing.B) {
	re := regexp.MustCompile(benchmarkPattern)
	schema, _ := ExtractSchema(re)

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
	re := regexp.MustCompile(benchmarkPattern)
	schema, _ := ExtractSchema(re)

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
	re := regexp.MustCompile(benchmarkPattern)
	schema, _ := ExtractSchema(re)

	lines := generateLogLines(10000, 0.80)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 10000)
		processLines(lines, re, writer)
	}
}

// ============================================================================
// Scanner-Specific Benchmarks
// ============================================================================

// BenchmarkScanner_Simple tests Scanner with simple 3-field pattern
func BenchmarkScanner_Simple(b *testing.B) {
	scanner, _ := NewExtractor(benchmarkPattern)
	line := []byte("2023-01-01T10:00:00.123Z INFO This is a test log message")
	out := make([][]byte, len(scanner.fields))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		scanner.Scan(line, out)
	}
}

// BenchmarkScanner_ByteSlice tests Scanner performance on byte slice input
func BenchmarkScanner_ByteSlice(b *testing.B) {
	scanner, _ := NewExtractor(benchmarkPattern)
	lines := generateLogLinesAsBytes(10000, 0.95)
	out := make([][]byte, len(scanner.fields))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			scanner.Scan(line, out)
		}
	}
}

// BenchmarkScanner_HighMatchRate tests Scanner with 100% match rate
func BenchmarkScanner_HighMatchRate(b *testing.B) {
	scanner, _ := NewExtractor(benchmarkPattern)
	lines := generateLogLinesAsBytes(10000, 1.0)
	out := make([][]byte, len(scanner.fields))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			scanner.Scan(line, out)
		}
	}
}

// BenchmarkScanner_LowMatchRate tests Scanner with 50% match rate
func BenchmarkScanner_LowMatchRate(b *testing.B) {
	scanner, _ := NewExtractor(benchmarkPattern)
	lines := generateLogLinesAsBytes(10000, 0.5)
	out := make([][]byte, len(scanner.fields))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			scanner.Scan(line, out)
		}
	}
}

// ============================================================================
// Writer-Specific Benchmarks
// ============================================================================

// BenchmarkWriter_WriteLines tests Writer batch processing
func BenchmarkWriter_WriteLines(b *testing.B) {
	ext, _ := NewExtractor(benchmarkPattern)
	scanner := ext.Scanner(Options{ZeroCopy: true})
	lines := generateLogLinesAsBytes(1000, 0.95)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewWriter(ext.Schema(), memory.DefaultAllocator, 1000)
		rec, _ := writer.WriteLinesSIMD(lines, scanner)
		if rec != nil {
			rec.Release()
		}
	}
}

// BenchmarkWriter_WriteLines_MultipleBatches tests Writer with small batch size
func BenchmarkWriter_WriteLines_MultipleBatches(b *testing.B) {
	ext, _ := NewExtractor(benchmarkPattern)
	scanner := ext.Scanner(Options{ZeroCopy: true})
	lines := generateLogLinesAsBytes(1000, 0.95)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewWriter(ext.Schema(), memory.DefaultAllocator, 100)
		for j := 0; j < len(lines); j += 100 {
			end := j + 100
			if end > len(lines) {
				end = len(lines)
			}
			rec, _ := writer.WriteLinesSIMD(lines[j:end], scanner)
			if rec != nil {
				rec.Release()
			}
		}
	}
}

// ============================================================================
// Pathological Input Benchmarks
// ============================================================================

// BenchmarkPathologicalInput tests performance on difficult input
func BenchmarkPathologicalInput(b *testing.B) {
	re := regexp.MustCompile(benchmarkPattern)
	schema, _ := ExtractSchema(re)

	lines := generatePathologicalLines(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 1000)
		processLines(lines, re, writer)
	}
}

// BenchmarkScanner_Pathological tests Scanner on pathological input
func BenchmarkScanner_Pathological(b *testing.B) {
	scanner, _ := NewExtractor(benchmarkPattern)
	lines := generatePathologicalLinesAsBytes(1000)
	out := make([][]byte, len(scanner.fields))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			scanner.Scan(line, out)
		}
	}
}

// ============================================================================
// Complex Pattern Benchmarks
// ============================================================================

// BenchmarkComplexPattern tests performance with complex regex patterns
func BenchmarkComplexPattern(b *testing.B) {
	re := regexp.MustCompile(`^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) \[(?P<thread>[^\]]+)\] (?P<level>\w+) +(?P<logger>[^ ]+) - (?P<message>.+)`)
	schema, _ := ExtractSchema(re)

	lines := generateComplexLogLines(1000, 0.90)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 1000)
		processLines(lines, re, writer)
	}
}

// BenchmarkScanner_ComplexPattern tests Scanner with complex pattern
func BenchmarkScanner_ComplexPattern(b *testing.B) {
	scanner, _ := NewExtractor(`^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) \[(?P<thread>[^\]]+)\] (?P<level>\w+) +(?P<logger>[^ ]+) - (?P<message>.+)`)
	lines := generateComplexLogLinesAsBytes(1000, 0.90)
	out := make([][]byte, len(scanner.fields))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			scanner.Scan(line, out)
		}
	}
}

// ============================================================================
// Flush Overhead Benchmarks
// ============================================================================

// BenchmarkFlushingOverhead tests the overhead of frequent flushing
func BenchmarkFlushingOverhead(b *testing.B) {
	re := regexp.MustCompile(benchmarkPattern)
	schema, _ := ExtractSchema(re)

	lines := generateLogLines(1000, 0.95)

	b.Run("SmallBatches", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer := NewArrowWriter(schema, memory.DefaultAllocator, 10)
			processLines(lines, re, writer)
		}
	})

	b.Run("LargeBatches", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer := NewArrowWriter(schema, memory.DefaultAllocator, 10000)
			processLines(lines, re, writer)
		}
	})
}

// ============================================================================
// Memory & Schema Benchmarks
// ============================================================================

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

// BenchmarkMemoryAllocators compares different memory allocators
func BenchmarkMemoryAllocators(b *testing.B) {
	re := regexp.MustCompile(benchmarkPattern)
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

// ============================================================================
// Legacy ParseLine Benchmarks
// ============================================================================

// BenchmarkParseLine tests line parsing performance
func BenchmarkParseLine(b *testing.B) {
	re := regexp.MustCompile(benchmarkPattern)
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

// ============================================================================
// ArrowWriter Benchmarks
// ============================================================================

// BenchmarkArrowWriterAppend tests the append operation performance
func BenchmarkArrowWriterAppend(b *testing.B) {
	re := regexp.MustCompile(`^(?P<level>\w+) (?P<message>.+)`)
	schema, _ := ExtractSchema(re)
	writer := NewArrowWriter(schema, memory.DefaultAllocator, 100000)

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

		values := []string{"INFO", "Test message"}
		for j := 0; j < 1000; j++ {
			writer.Append(values)
		}

		b.StartTimer()
		record := writer.Flush()
		record.Release()
	}
}

// ============================================================================
// Helper functions for benchmark data generation
// ============================================================================

func generateLogLines(count int, matchRate float64) []string {
	lines := make([]string, count)
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}

	for i := 0; i < count; i++ {
		if float64(i)/float64(count) < matchRate {
			level := levels[i%len(levels)]
			lines[i] = "2023-01-01T10:00:00.000Z " + level + " This is log message " + string(rune('A'+i%26))
		} else {
			lines[i] = "invalid log line " + string(rune('A'+i%26))
		}
	}

	return lines
}

func generateLogLinesAsBytes(count int, matchRate float64) [][]byte {
	lines := make([][]byte, count)
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}

	for i := 0; i < count; i++ {
		if float64(i)/float64(count) < matchRate {
			level := levels[i%len(levels)]
			lines[i] = []byte("2023-01-01T10:00:00.000Z " + level + " This is log message " + string(rune('A'+i%26)))
		} else {
			lines[i] = []byte("invalid log line " + string(rune('A'+i%26)))
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

func generateComplexLogLinesAsBytes(count int, matchRate float64) [][]byte {
	lines := make([][]byte, count)
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	threads := []string{"main", "worker-1", "worker-2", "scheduler"}
	loggers := []string{"com.example.Service", "com.example.Controller", "com.example.Repository"}

	for i := 0; i < count; i++ {
		if float64(i)/float64(count) < matchRate {
			level := levels[i%len(levels)]
			thread := threads[i%len(threads)]
			logger := loggers[i%len(loggers)]
			lines[i] = []byte("2023-01-01T10:00:00.123Z [" + thread + "] " + level + " " + logger + " - Processing request " + string(rune('A'+i%26)))
		} else {
			lines[i] = []byte("malformed log entry")
		}
	}

	return lines
}

func generatePathologicalLines(count int) []string {
	lines := make([]string, count)

	for i := 0; i < count; i++ {
		switch i % 10 {
		case 0, 1, 2:
			lines[i] = ""
		case 3, 4:
			lines[i] = "2023-01-01 INFO " + strings.Repeat("very long message ", 100)
		case 5, 6, 7:
			lines[i] = "this does not match the pattern at all"
		default:
			lines[i] = "2023-01-01 INFO valid message"
		}
	}

	return lines
}

func generatePathologicalLinesAsBytes(count int) [][]byte {
	lines := make([][]byte, count)

	for i := 0; i < count; i++ {
		switch i % 10 {
		case 0, 1, 2:
			lines[i] = []byte("")
		case 3, 4:
			lines[i] = []byte("2023-01-01 INFO " + strings.Repeat("very long message ", 100))
		case 5, 6, 7:
			lines[i] = []byte("this does not match the pattern at all")
		default:
			lines[i] = []byte("2023-01-01 INFO valid message")
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

	if writer.Rows() > 0 {
		rec := writer.Flush()
		rec.Release()
	}
}
