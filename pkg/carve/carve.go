package carve

import (
	"errors"
	"regexp"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ExtractSchema builds an Arrow schema from the named capture groups in the regex.
func ExtractSchema(re *regexp.Regexp) (*arrow.Schema, error) {
	if re == nil {
		return nil, errors.New("nil regexp")
	}
	var fields []arrow.Field
	names := re.SubexpNames()
	for i := 1; i < len(names); i++ {
		name := names[i]
		if name == "" {
			continue
		}
		fields = append(fields, arrow.Field{Name: name, Type: arrow.BinaryTypes.String})
	}
	if len(fields) == 0 {
		return nil, errors.New("pattern must contain named capture groups")
	}
	return arrow.NewSchema(fields, nil), nil
}

// ParseLine returns regex capture groups for the line. Non-matching lines return nil.
func ParseLine(line string, re *regexp.Regexp) []string {
	return ExtractValues[string](line, re)
}

// ExtractValues returns regex capture groups for the line as either strings or
// byte slices depending on the type parameter. The slice capacity matches the
// number of capture groups to minimize allocations. Non-matching lines return
// nil.
func ExtractValues[T ~string | ~[]byte](line T, re *regexp.Regexp) []T {
	if re == nil {
		return nil
	}

	switch any(line).(type) {
	case string:
		m := re.FindStringSubmatch(string(line))
		if m == nil {
			return nil
		}
		out := make([]T, 0, len(m)-1)
		for i := 1; i < len(m); i++ {
			out = append(out, T(m[i]))
		}
		return out
	case []byte:
		m := re.FindSubmatch([]byte(line))
		if m == nil {
			return nil
		}
		out := make([]T, 0, len(m)-1)
		for i := 1; i < len(m); i++ {
			out = append(out, T(m[i]))
		}
		return out
	default:
		return nil
	}
}

// ArrowWriter writes record batches with flushing to limit memory usage.
type ArrowWriter struct {
	schema      *arrow.Schema
	builders    []*array.StringBuilder
	mem         memory.Allocator
	maxRows     int
	rowsInBatch int
}

// NewArrowWriter creates a writer that buffers rows up to maxRows per batch.
func NewArrowWriter(schema *arrow.Schema, mem memory.Allocator, maxRows int) *ArrowWriter {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	builders := make([]*array.StringBuilder, len(schema.Fields()))
	for i := range builders {
		builders[i] = array.NewStringBuilder(mem)
	}
	return &ArrowWriter{schema: schema, builders: builders, mem: mem, maxRows: maxRows}
}

// Append adds a row of string values to the current batch.
func (w *ArrowWriter) Append(values []string) {
	for i, v := range values {
		w.builders[i].Append(v)
	}
	w.rowsInBatch++
}

// ShouldFlush returns true if the buffered rows exceed the flush interval.
func (w *ArrowWriter) ShouldFlush() bool {
	return w.maxRows > 0 && w.rowsInBatch >= w.maxRows
}

// Rows returns the number of rows currently buffered.
func (w *ArrowWriter) Rows() int {
	return w.rowsInBatch
}

// Flush returns a record batch containing buffered rows and resets builders.
func (w *ArrowWriter) Flush() arrow.Record {
	arrays := make([]arrow.Array, len(w.builders))
	for i, b := range w.builders {
		arrays[i] = b.NewArray()
		b.Release()
		w.builders[i] = array.NewStringBuilder(w.mem)
	}
	rec := array.NewRecord(w.schema, arrays, int64(w.rowsInBatch))
	for _, arr := range arrays {
		arr.Release()
	}
	w.rowsInBatch = 0
	return rec
}
