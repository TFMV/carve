package carve

import (
	"regexp"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkParseLine measures the overhead of regex parsing with capture slice reuse.
func BenchmarkParseLine(b *testing.B) {
	re := regexp.MustCompile(`^(?P<ts>[^ ]+) (?P<level>[A-Z]+) (?P<msg>.+)`)
	line := "2024-01-01T00:00:00Z INFO hello"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ExtractValues[string](line, re)
	}
}

// BenchmarkArrowAppend measures Arrow builder append performance.
func BenchmarkArrowAppend(b *testing.B) {
	re := regexp.MustCompile(`^(?P<ts>[^ ]+) (?P<level>[A-Z]+) (?P<msg>.+)`)
	schema, _ := ExtractSchema(re)
	writer := NewArrowWriter(schema, memory.DefaultAllocator, 1_000_000)
	values := []string{"2024-01-01T00:00:00Z", "INFO", "msg"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Append(values)
		writer.rowsInBatch = 0
	}
}

// BenchmarkEndToEndSmall parses and writes a small dataset (10K lines).
func BenchmarkEndToEndSmall(b *testing.B) {
	re := regexp.MustCompile(`^(?P<ts>[^ ]+) (?P<level>[A-Z]+) (?P<msg>.+)`)
	schema, _ := ExtractSchema(re)
	lines := generateLogLines(10_000, 1.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 10_000)
		for _, line := range lines {
			vals := ExtractValues[string](line, re)
			if vals != nil {
				writer.Append(vals)
			}
		}
		if writer.Rows() > 0 {
			rec := writer.Flush()
			rec.Release()
		}
	}
}

// BenchmarkEndToEndLarge parses and writes a large dataset (1M lines).
func BenchmarkEndToEndLarge(b *testing.B) {
	re := regexp.MustCompile(`^(?P<ts>[^ ]+) (?P<level>[A-Z]+) (?P<msg>.+)`)
	schema, _ := ExtractSchema(re)
	lines := generateLogLines(1_000_000, 1.0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 50_000)
		for _, line := range lines {
			vals := ExtractValues[string](line, re)
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
}
