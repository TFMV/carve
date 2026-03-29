package carve

import (
	"bytes"
	"errors"
	"regexp"
	"regexp/syntax"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ============================================================
// Public API
// ============================================================

// New creates a high-performance scanner from a regex pattern.
// Only named capture groups are supported.
func New(pattern string) (*Scanner, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	schema, err := ExtractSchema(re)
	if err != nil {
		return nil, err
	}

	names := re.SubexpNames()
	numFields := 0
	for i := 1; i < len(names); i++ {
		if names[i] != "" {
			numFields++
		}
	}

	plan, err := buildScanPlan(re, numFields)
	if err != nil {
		return nil, err
	}

	return &Scanner{
		schema:  schema,
		fields:  plan,
		scratch: make([][]byte, len(plan)),
		opts:    Options{ZeroCopy: true},
	}, nil
}

// NewExtractor is an alias for New for API compatibility.
func NewExtractor(pattern string) (*Scanner, error) {
	return New(pattern)
}

type Scanner struct {
	schema  *arrow.Schema
	fields  []fieldVM
	scratch [][]byte
	opts    Options
}

type Options struct {
	ZeroCopy bool
}

// Scan writes captured fields into `out`. Returns true if at least one field was extracted.
// The returned slices are valid only until the next Scan call when ZeroCopy=true.
func (s *Scanner) Scan(line []byte, out [][]byte) bool {
	if len(out) < len(s.fields) {
		return false
	}

	pos := 0
	n := len(line)

	for i, f := range s.fields {
		if f.isLast {
			if pos < n {
				out[i] = slice(line, pos, n, s.opts.ZeroCopy)
			} else {
				out[i] = nil
			}
			return true
		}

		idx := bytes.IndexByte(line[pos:], f.delim)
		if idx < 0 {
			out[i] = nil
			pos = n
		} else {
			out[i] = slice(line, pos, pos+idx, s.opts.ZeroCopy)
			pos += idx + 1
		}
	}
	return true
}

func (s *Scanner) Schema() *arrow.Schema { return s.schema }

// Scanner returns a new scanner with the given options (for chaining).
func (s *Scanner) Scanner(opts Options) *Scanner {
	s.opts = opts
	return s
}

// WithOptions is an alias for Scanner.
func (s *Scanner) WithOptions(opts Options) *Scanner {
	return s.Scanner(opts)
}

// ============================================================
// Writer (batch builder)
// ============================================================

type Writer struct {
	schema      *arrow.Schema
	mem         memory.Allocator
	maxRows     int
	builders    []*array.BinaryBuilder
	scratch     [][]byte
	tempColVals [][][]byte
	tempValids  [][]bool
	rows        int
}

func NewWriter(schema *arrow.Schema, mem memory.Allocator, maxRows int) *Writer {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	if maxRows <= 0 {
		maxRows = 8192
	}

	numCols := len(schema.Fields())
	builders := make([]*array.BinaryBuilder, numCols)
	for i := range builders {
		builders[i] = array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	}

	tempColVals := make([][][]byte, numCols)
	tempValids := make([][]bool, numCols)
	for i := range tempColVals {
		tempColVals[i] = make([][]byte, 0, maxRows)
		tempValids[i] = make([]bool, 0, maxRows)
	}

	return &Writer{
		schema:      schema,
		mem:         mem,
		maxRows:     maxRows,
		builders:    builders,
		scratch:     make([][]byte, numCols),
		tempColVals: tempColVals,
		tempValids:  tempValids,
	}
}

func (w *Writer) WriteLinesSIMD(lines [][]byte, s *Scanner) (arrow.Record, error) {
	builders := w.builders
	scratch := w.scratch
	numCols := len(scratch)
	maxRows := w.maxRows
	rows := w.rows

	for i := range w.tempColVals {
		w.tempColVals[i] = w.tempColVals[i][:0]
		w.tempValids[i] = w.tempValids[i][:0]
	}

	for _, line := range lines {
		if !s.Scan(line, scratch) {
			continue
		}

		for i := 0; i < numCols; i++ {
			val := scratch[i]
			w.tempColVals[i] = append(w.tempColVals[i], val)
			w.tempValids[i] = append(w.tempValids[i], val != nil)
		}

		rows++
		if rows >= maxRows {
			w.rows = rows
			for i := 0; i < numCols; i++ {
				if n := totalDataLen(w.tempColVals[i]); n > 0 {
					builders[i].ReserveData(n)
				}
				builders[i].AppendValues(w.tempColVals[i], w.tempValids[i])
			}
			return w.Flush()
		}
	}

	w.rows = rows
	if len(w.tempColVals[0]) > 0 {
		for i := 0; i < numCols; i++ {
			if n := totalDataLen(w.tempColVals[i]); n > 0 {
				builders[i].ReserveData(n)
			}
			builders[i].AppendValues(w.tempColVals[i], w.tempValids[i])
		}
	}
	return nil, nil
}

func totalDataLen(vals [][]byte) int {
	n := 0
	for _, v := range vals {
		n += len(v)
	}
	return n
}

// Flush returns the current batch and resets the writer.
// The returned Record must be released by the caller.
func (w *Writer) Flush() (arrow.Record, error) {
	if w.rows == 0 {
		return nil, nil
	}

	arrs := make([]arrow.Array, len(w.builders))
	for i, b := range w.builders {
		arrs[i] = b.NewArray()
	}

	rec := array.NewRecord(w.schema, arrs, int64(w.rows))

	for _, a := range arrs {
		a.Release()
	}

	w.rows = 0
	return rec, nil
}

// ============================================================
// ArrowWriter (legacy API for backward compatibility)
// ============================================================

type ArrowWriter struct {
	schema      *arrow.Schema
	builders    []*array.BinaryBuilder
	mem         memory.Allocator
	maxRows     int
	rowsInBatch int
}

func NewArrowWriter(schema *arrow.Schema, mem memory.Allocator, maxRows int) *ArrowWriter {
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	builders := make([]*array.BinaryBuilder, len(schema.Fields()))
	for i := range builders {
		builders[i] = array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	}
	return &ArrowWriter{schema: schema, builders: builders, mem: mem, maxRows: maxRows}
}

func (w *ArrowWriter) Append(values []string) {
	for i, v := range values {
		w.builders[i].Append([]byte(v))
	}
	w.rowsInBatch++
}

func (w *ArrowWriter) ShouldFlush() bool {
	return w.maxRows > 0 && w.rowsInBatch >= w.maxRows
}

func (w *ArrowWriter) Rows() int {
	return w.rowsInBatch
}

func (w *ArrowWriter) Flush() arrow.Record {
	arrays := make([]arrow.Array, len(w.builders))
	for i, b := range w.builders {
		arrays[i] = b.NewArray()
	}
	rec := array.NewRecord(w.schema, arrays, int64(w.rowsInBatch))
	for _, arr := range arrays {
		arr.Release()
	}
	w.rowsInBatch = 0
	return rec
}

// ============================================================
// Internal helpers
// ============================================================

type fieldVM struct {
	delim  byte
	isLast bool
}

func buildScanPlan(re *regexp.Regexp, numFields int) ([]fieldVM, error) {
	ast, err := syntax.Parse(re.String(), syntax.Perl)
	if err != nil {
		return nil, err
	}

	fields := make([]fieldVM, 0, numFields)
	var literals []byte

	if ast.Op == syntax.OpConcat {
		for _, n := range ast.Sub {
			if n.Op == syntax.OpLiteral && len(n.Rune) > 0 {
				literals = append(literals, byte(n.Rune[0]))
			}
		}
	}

	litIdx := 0
	for i := 0; i < numFields; i++ {
		vm := fieldVM{}
		if litIdx < len(literals) {
			vm.delim = literals[litIdx]
			litIdx++
		} else {
			vm.isLast = true
		}
		fields = append(fields, vm)
	}
	return fields, nil
}

// ExtractSchema is kept for external use (uses Binary for raw captures).
func ExtractSchema(re *regexp.Regexp) (*arrow.Schema, error) {
	if re == nil {
		return nil, errors.New("nil regexp")
	}
	names := re.SubexpNames()
	fields := make([]arrow.Field, 0, len(names))
	for i := 1; i < len(names); i++ {
		if names[i] != "" {
			fields = append(fields, arrow.Field{
				Name: names[i],
				Type: arrow.BinaryTypes.Binary,
			})
		}
	}
	if len(fields) == 0 {
		return nil, errors.New("pattern must contain named capture groups")
	}
	return arrow.NewSchema(fields, nil), nil
}

// Legacy helpers (kept for compatibility)
func ParseLine(line string, re *regexp.Regexp) []string {
	return ExtractValues[string](line, re)
}

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

// slice helper
func slice(b []byte, start, end int, zero bool) []byte {
	if zero {
		return b[start:end]
	}
	cp := make([]byte, end-start)
	copy(cp, b[start:end])
	return cp
}
