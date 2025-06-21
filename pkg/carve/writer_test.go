package carve

import (
	"regexp"
	"testing"

	"github.com/apache/arrow/go/v18/arrow/memory"
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
