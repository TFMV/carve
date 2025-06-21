package carve

import (
	"regexp"
	"testing"
)

func TestExtractSchema(t *testing.T) {
	re := regexp.MustCompile(`(?P<a>\w+)-(?P<b>\d+)`)
	schema, err := ExtractSchema(re)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(schema.Fields()) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(schema.Fields()))
	}
	if schema.Field(0).Name != "a" || schema.Field(1).Name != "b" {
		t.Fatalf("unexpected field order: %v", schema)
	}
}

func TestParseLine(t *testing.T) {
	re := regexp.MustCompile(`(?P<a>\w+)-(?P<b>\d+)`)
	vals := ParseLine("foo-123", re)
	if len(vals) != 2 || vals[0] != "foo" || vals[1] != "123" {
		t.Fatalf("unexpected values: %v", vals)
	}
	if ParseLine("bad", re) != nil {
		t.Fatalf("expected nil for non-match")
	}
}
