package main

import (
	"os"
	"os/exec"
	"regexp"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestCLI(t *testing.T) {
	tmp, err := os.CreateTemp("", "out.arrow")
	if err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	// Updated pattern to match the new log format (timestamp starting with year)
	cmd := exec.Command("go", "run", ".", "--pattern", `^(?P<ts>\d{4}-[^ ]+) (?P<level>\w+) (?P<msg>.+)`, "--input", "../../testdata/sample.log", "--output", tmp.Name())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("run failed: %v: %s", err, out)
	}

	if _, err := os.Stat(tmp.Name()); err != nil {
		t.Fatalf("output not created: %v", err)
	}

	// Validate the Arrow file can be read
	validateArrowFile(t, tmp.Name())
}

func TestCLI_SchemaFlag(t *testing.T) {
	cmd := exec.Command("go", "run", ".", "--pattern", `^(?P<timestamp>[^ ]+) (?P<level>\w+) (?P<message>.+)`, "--schema")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("schema command failed: %v: %s", err, out)
	}

	output := string(out)
	if !regexp.MustCompile(`Schema \(3 fields\):`).MatchString(output) {
		t.Fatalf("unexpected schema output: %s", output)
	}
	if !regexp.MustCompile(`timestamp.*(utf8|string)`).MatchString(output) {
		t.Fatalf("missing timestamp field in schema: %s", output)
	}
}

func TestCLI_VerboseFlag(t *testing.T) {
	tmp, err := os.CreateTemp("", "out.arrow")
	if err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	cmd := exec.Command("go", "run", ".", "--pattern", `^(?P<ts>\d{4}-[^ ]+) (?P<level>\w+) (?P<msg>.+)`, "--input", "../../testdata/sample.log", "--output", tmp.Name(), "--verbose")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verbose run failed: %v: %s", err, out)
	}

	output := string(out)
	// Should contain warnings for malformed lines
	if !regexp.MustCompile(`\[warn\] line \d+: does not match pattern`).MatchString(output) {
		t.Fatalf("expected warning messages in verbose output: %s", output)
	}
	// Should contain processing summary
	if !regexp.MustCompile(`processed \d+ lines, wrote \d+ rows`).MatchString(output) {
		t.Fatalf("expected processing summary in verbose output: %s", output)
	}
}

func TestCLI_StdinInput(t *testing.T) {
	tmp, err := os.CreateTemp("", "out.arrow")
	if err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	cmd := exec.Command("go", "run", ".", "--pattern", `^(?P<ts>\d{4}-[^ ]+) (?P<level>\w+) (?P<msg>.+)`, "--output", tmp.Name())
	cmd.Stdin = mustOpen(t, "../../testdata/sample.log")

	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("stdin run failed: %v: %s", err, out)
	}

	validateArrowFile(t, tmp.Name())
}

func TestCLI_FlushInterval(t *testing.T) {
	tmp, err := os.CreateTemp("", "out.arrow")
	if err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	// Use small flush interval to test batching
	cmd := exec.Command("go", "run", ".", "--pattern", `^(?P<ts>\d{4}-[^ ]+) (?P<level>\w+) (?P<msg>.+)`, "--input", "../../testdata/sample.log", "--output", tmp.Name(), "--flush-interval", "2")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("flush interval run failed: %v: %s", err, out)
	}

	validateArrowFile(t, tmp.Name())
}

func TestCLI_ErrorHandling(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{
			name:    "missing pattern",
			args:    []string{"--output", "test.arrow"},
			wantErr: true,
		},
		{
			name:    "missing output",
			args:    []string{"--pattern", "(?P<a>\\w+)"},
			wantErr: true,
		},
		{
			name:    "invalid pattern",
			args:    []string{"--pattern", "[invalid", "--output", "test.arrow"},
			wantErr: true,
		},
		{
			name:    "pattern without named groups",
			args:    []string{"--pattern", "(\\w+)", "--output", "test.arrow"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("go", "run", ".")
			cmd.Args = append(cmd.Args, tt.args...)
			err := cmd.Run()

			if tt.wantErr && err == nil {
				t.Fatalf("expected error but command succeeded")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// Helper functions

func validateArrowFile(t *testing.T, filename string) {
	t.Helper()

	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("failed to open arrow file: %v", err)
	}
	defer f.Close()

	reader, err := ipc.NewFileReader(f, ipc.WithAllocator(memory.DefaultAllocator))
	if err != nil {
		t.Fatalf("failed to create arrow reader: %v", err)
	}
	defer reader.Close()

	// Validate schema
	schema := reader.Schema()
	if schema == nil {
		t.Fatal("schema is nil")
	}

	expectedFields := []string{"ts", "level", "msg"}
	if len(schema.Fields()) != len(expectedFields) {
		t.Fatalf("expected %d fields, got %d", len(expectedFields), len(schema.Fields()))
	}

	for i, expectedName := range expectedFields {
		if schema.Field(i).Name != expectedName {
			t.Fatalf("field %d: expected %q, got %q", i, expectedName, schema.Field(i).Name)
		}
	}

	// Validate records
	totalRows := int64(0)
	for i := 0; i < reader.NumRecords(); i++ {
		rec, err := reader.Record(i)
		if err != nil {
			t.Fatalf("failed to read record %d: %v", i, err)
		}
		if rec.NumCols() != 3 {
			t.Fatalf("expected 3 columns, got %d", rec.NumCols())
		}
		totalRows += rec.NumRows()
		rec.Release()
	}

	// We expect 9 valid log lines from the sample.log (excluding 2 malformed lines)
	// The pattern should match lines starting with timestamp format like "2023-01-01T..."
	expectedRows := int64(9)
	if totalRows != expectedRows {
		t.Fatalf("expected %d rows, got %d", expectedRows, totalRows)
	}
}

func mustOpen(t *testing.T, filename string) *os.File {
	t.Helper()
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("failed to open %s: %v", filename, err)
	}
	return f
}
