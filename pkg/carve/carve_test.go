package carve

import (
	"regexp"
	"testing"
)

func TestExtractSchema(t *testing.T) {
	tests := []struct {
		name       string
		pattern    string
		wantFields int
		wantNames  []string
		wantError  bool
	}{
		{
			name:       "simple pattern",
			pattern:    `(?P<a>\w+)-(?P<b>\d+)`,
			wantFields: 2,
			wantNames:  []string{"a", "b"},
		},
		{
			name:       "complex log pattern",
			pattern:    `^(?P<timestamp>[^ ]+) (?P<level>\w+) (?P<message>.+)`,
			wantFields: 3,
			wantNames:  []string{"timestamp", "level", "message"},
		},
		{
			name:       "web server log pattern",
			pattern:    `^(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>\w+) (?P<path>\S+) (?P<protocol>[^"]+)" (?P<status>\d+) (?P<size>\d+)`,
			wantFields: 7,
			wantNames:  []string{"ip", "timestamp", "method", "path", "protocol", "status", "size"},
		},
		{
			name:       "single field",
			pattern:    `(?P<field>.+)`,
			wantFields: 1,
			wantNames:  []string{"field"},
		},
		{
			name:      "no named groups",
			pattern:   `(\w+)-(\d+)`,
			wantError: true,
		},
		{
			name:       "mixed named and unnamed groups",
			pattern:    `(?P<name>\w+)-(\d+)-(?P<value>\w+)`,
			wantFields: 2,
			wantNames:  []string{"name", "value"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re := regexp.MustCompile(tt.pattern)
			schema, err := ExtractSchema(re)

			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(schema.Fields()) != tt.wantFields {
				t.Fatalf("expected %d fields, got %d", tt.wantFields, len(schema.Fields()))
			}

			for i, expectedName := range tt.wantNames {
				if schema.Field(i).Name != expectedName {
					t.Fatalf("field %d: expected name %s, got %s", i, expectedName, schema.Field(i).Name)
				}
			}
		})
	}
}

func TestParseLine(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		line    string
		want    []string
		wantNil bool
	}{
		{
			name:    "simple match",
			pattern: `(?P<a>\w+)-(?P<b>\d+)`,
			line:    "foo-123",
			want:    []string{"foo", "123"},
		},
		{
			name:    "simple non-match",
			pattern: `(?P<a>\w+)-(?P<b>\d+)`,
			line:    "bad",
			wantNil: true,
		},
		{
			name:    "log line match",
			pattern: `^(?P<timestamp>[^ ]+) (?P<level>\w+) (?P<message>.+)`,
			line:    "2023-01-01T10:00:00.123Z INFO Application started successfully",
			want:    []string{"2023-01-01T10:00:00.123Z", "INFO", "Application started successfully"},
		},
		{
			name:    "log line non-match",
			pattern: `^(?P<timestamp>\d{4}-[^ ]+) (?P<level>\w+) (?P<message>.+)`,
			line:    "invalid log format",
			wantNil: true,
		},
		{
			name:    "empty line",
			pattern: `(?P<field>.+)`,
			line:    "",
			wantNil: true,
		},
		{
			name:    "whitespace only line",
			pattern: `(?P<field>.+)`,
			line:    "   ",
			want:    []string{"   "},
		},
		{
			name:    "special characters",
			pattern: `(?P<name>[^:]+):(?P<value>.+)`,
			line:    "key with spaces:value with @#$%^&*(){}[]",
			want:    []string{"key with spaces", "value with @#$%^&*(){}[]"},
		},
		{
			name:    "unicode characters",
			pattern: `(?P<name>[^:]+):(?P<value>.+)`,
			line:    "测试:こんにちは世界",
			want:    []string{"测试", "こんにちは世界"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re := regexp.MustCompile(tt.pattern)
			vals := ParseLine(tt.line, re)

			if tt.wantNil {
				if vals != nil {
					t.Fatalf("expected nil but got: %v", vals)
				}
				return
			}

			if len(vals) != len(tt.want) {
				t.Fatalf("expected %d values, got %d: %v", len(tt.want), len(vals), vals)
			}

			for i, expected := range tt.want {
				if vals[i] != expected {
					t.Fatalf("value %d: expected %q, got %q", i, expected, vals[i])
				}
			}
		})
	}
}
