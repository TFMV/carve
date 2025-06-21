package carve

import (
	"regexp"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestArrowWriterFlush(t *testing.T) {
	re := regexp.MustCompile(`(?P<a>\w+)`)
	schema, _ := ExtractSchema(re)
	w := NewArrowWriter(schema, memory.DefaultAllocator, 2)
	w.Append([]string{"x"})
	if w.ShouldFlush() {
		t.Fatalf("should not flush yet")
	}
	w.Append([]string{"y"})
	if !w.ShouldFlush() {
		t.Fatalf("should flush")
	}
	rec := w.Flush()
	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}
	rec.Release()
}

func TestArrowWriter(t *testing.T) {
	tests := []struct {
		name        string
		pattern     string
		batchSize   int
		data        [][]string
		wantBatches int
		wantRows    []int64
	}{
		{
			name:        "single field small batch",
			pattern:     `(?P<field>\w+)`,
			batchSize:   2,
			data:        [][]string{{"a"}, {"b"}, {"c"}},
			wantBatches: 2,
			wantRows:    []int64{2, 1},
		},
		{
			name:        "multiple fields",
			pattern:     `(?P<name>\w+):(?P<value>\d+)`,
			batchSize:   3,
			data:        [][]string{{"foo", "123"}, {"bar", "456"}, {"baz", "789"}, {"qux", "000"}},
			wantBatches: 2,
			wantRows:    []int64{3, 1},
		},
		{
			name:        "exact batch size",
			pattern:     `(?P<field>\w+)`,
			batchSize:   2,
			data:        [][]string{{"a"}, {"b"}},
			wantBatches: 1,
			wantRows:    []int64{2},
		},
		{
			name:        "large batch size",
			pattern:     `(?P<field>\w+)`,
			batchSize:   1000,
			data:        [][]string{{"a"}, {"b"}, {"c"}},
			wantBatches: 1,
			wantRows:    []int64{3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re := regexp.MustCompile(tt.pattern)
			schema, err := ExtractSchema(re)
			if err != nil {
				t.Fatalf("failed to extract schema: %v", err)
			}

			writer := NewArrowWriter(schema, memory.DefaultAllocator, tt.batchSize)
			var records []arrow.Record

			// Append all data
			for _, row := range tt.data {
				writer.Append(row)
				if writer.ShouldFlush() {
					rec := writer.Flush()
					records = append(records, rec)
				}
			}

			// Final flush
			if writer.Rows() > 0 {
				rec := writer.Flush()
				records = append(records, rec)
			}

			// Verify results
			if len(records) != tt.wantBatches {
				t.Fatalf("expected %d batches, got %d", tt.wantBatches, len(records))
			}

			for i, rec := range records {
				if rec.NumRows() != tt.wantRows[i] {
					t.Fatalf("batch %d: expected %d rows, got %d", i, tt.wantRows[i], rec.NumRows())
				}
				rec.Release()
			}
		})
	}
}

func TestArrowWriterMemoryManagement(t *testing.T) {
	re := regexp.MustCompile(`(?P<field>\w+)`)
	schema, _ := ExtractSchema(re)

	// Test with custom allocator
	allocator := memory.NewGoAllocator()
	writer := NewArrowWriter(schema, allocator, 2)

	// Add some data
	writer.Append([]string{"test1"})
	writer.Append([]string{"test2"})

	// Should be ready to flush
	if !writer.ShouldFlush() {
		t.Fatalf("should be ready to flush")
	}

	// Flush and verify
	rec := writer.Flush()
	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}

	// Verify we can continue adding after flush
	writer.Append([]string{"test3"})
	if writer.Rows() != 1 {
		t.Fatalf("expected 1 row after flush, got %d", writer.Rows())
	}

	rec.Release()

	// Final flush
	if writer.Rows() > 0 {
		finalRec := writer.Flush()
		if finalRec.NumRows() != 1 {
			t.Fatalf("expected 1 row in final flush, got %d", finalRec.NumRows())
		}
		finalRec.Release()
	}
}

func TestArrowWriterEdgeCases(t *testing.T) {
	re := regexp.MustCompile(`(?P<field>.*)`)
	schema, _ := ExtractSchema(re)

	t.Run("empty values", func(t *testing.T) {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 10)
		writer.Append([]string{""})
		writer.Append([]string{"   "})
		writer.Append([]string{"normal"})

		rec := writer.Flush()
		if rec.NumRows() != 3 {
			t.Fatalf("expected 3 rows, got %d", rec.NumRows())
		}
		rec.Release()
	})

	t.Run("unicode values", func(t *testing.T) {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 10)
		writer.Append([]string{"测试"})
		writer.Append([]string{"こんにちは"})
		writer.Append([]string{"🚀"})

		rec := writer.Flush()
		if rec.NumRows() != 3 {
			t.Fatalf("expected 3 rows, got %d", rec.NumRows())
		}
		rec.Release()
	})

	t.Run("large values", func(t *testing.T) {
		writer := NewArrowWriter(schema, memory.DefaultAllocator, 10)
		largeValue := string(make([]byte, 10000))
		for i := range largeValue {
			largeValue = string(rune('A' + i%26))
		}
		writer.Append([]string{largeValue})

		rec := writer.Flush()
		if rec.NumRows() != 1 {
			t.Fatalf("expected 1 row, got %d", rec.NumRows())
		}
		rec.Release()
	})
}
