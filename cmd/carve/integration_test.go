package main

import (
	"os"
	"os/exec"
	"testing"
)

func TestCLI(t *testing.T) {
	tmp, err := os.CreateTemp("", "out.arrow")
	if err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	cmd := exec.Command("go", "run", "./cmd/carve", "--pattern", `^(?P<ts>[^ ]+) (?P<level>\w+) (?P<msg>.+)`, "--input", "testdata/sample.log", "--output", tmp.Name())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("run failed: %v: %s", err, out)
	}

	if _, err := os.Stat(tmp.Name()); err != nil {
		t.Fatalf("output not created: %v", err)
	}
}
