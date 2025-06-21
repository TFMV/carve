package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"carve/pkg/carve"
)

const version = "0.2.0"

func main() {
	pattern := flag.String("pattern", "", "regex pattern with named capture groups")
	input := flag.String("input", "", "input file (defaults to stdin)")
	output := flag.String("output", "", "output Arrow IPC file")
	flush := flag.Int("flush-interval", 10000, "rows per record batch")
	schemaOnly := flag.Bool("schema", false, "print inferred schema and exit")
	verbose := flag.Bool("verbose", false, "verbose logging")
	showVersion := flag.Bool("version", false, "print version and exit")
	benchReport := flag.Bool("bench-report", false, "emit per-batch timing information")
	maxRows := flag.Int("max-rows", 0, "limit processed input (0 = unlimited)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "carve - convert structured logs to Arrow format\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s --pattern '^(?P<ts>[^ ]+) (?P<level>\\w+) (?P<msg>.+)' --input app.log --output out.arrow\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --pattern '^(?P<ts>[^ ]+) (?P<level>\\w+) (?P<msg>.+)' --schema\n", os.Args[0])
	}

	flag.Parse()

	if *showVersion {
		fmt.Println("carve", version)
		return
	}
	if *pattern == "" {
		fmt.Fprintf(os.Stderr, "Error: --pattern flag is required\n\n")
		flag.Usage()
		os.Exit(1)
	}
	if *output == "" && !*schemaOnly {
		fmt.Fprintf(os.Stderr, "Error: --output flag is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	re, err := regexp.Compile(*pattern)
	if err != nil {
		log.Fatalf("failed to compile pattern: %v", err)
	}

	schema, err := carve.ExtractSchema(re)
	if err != nil {
		log.Fatalf("schema error: %v", err)
	}
	if *schemaOnly {
		printSchema(schema)
		return
	}

	mem := memory.DefaultAllocator
	writer := carve.NewArrowWriter(schema, mem, *flush)

	var r *os.File
	if *input != "" {
		f, err := os.Open(*input)
		if err != nil {
			log.Fatalf("failed to open input: %v", err)
		}
		r = f
		defer f.Close()
	} else {
		r = os.Stdin
	}

	// Create output file and IPC writer once
	outFile, err := os.Create(*output)
	if err != nil {
		log.Fatalf("failed to create output: %v", err)
	}
	defer outFile.Close()

	ipcWriter, err := ipc.NewFileWriter(outFile, ipc.WithSchema(schema))
	if err != nil {
		log.Fatalf("failed to create IPC writer: %v", err)
	}
	defer ipcWriter.Close()

	scanner := bufio.NewScanner(r)
	lineNum := 0
	totalRows := 0
	var batchStart time.Time
	if *benchReport {
		batchStart = time.Now()
	}

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Check max-rows limit
		if *maxRows > 0 && totalRows >= *maxRows {
			if *verbose {
				log.Printf("reached max-rows limit of %d", *maxRows)
			}
			break
		}

		vals := carve.ParseLine(line, re)
		if vals == nil {
			if *verbose {
				log.Printf("[warn] line %d: does not match pattern", lineNum)
			}
			continue
		}
		writer.Append(vals)
		totalRows++

		if writer.ShouldFlush() {
			rec := writer.Flush()
			if err := ipcWriter.Write(rec); err != nil {
				rec.Release()
				log.Fatalf("write error: %v", err)
			}
			if *benchReport {
				duration := time.Since(batchStart)
				log.Printf("[bench] batch: %d rows, %v", rec.NumRows(), duration)
				batchStart = time.Now()
			}
			rec.Release()
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("scan error: %v", err)
	}

	// Flush remaining rows
	if writer.Rows() > 0 {
		rec := writer.Flush()
		if err := ipcWriter.Write(rec); err != nil {
			rec.Release()
			log.Fatalf("write error: %v", err)
		}
		if *benchReport {
			duration := time.Since(batchStart)
			log.Printf("[bench] final batch: %d rows, %v", rec.NumRows(), duration)
		}
		rec.Release()
	}

	if *verbose {
		fmt.Printf("processed %d lines, wrote %d rows to %s\n", lineNum, totalRows, *output)
	}
}

func printSchema(schema *arrow.Schema) {
	fmt.Printf("Schema (%d fields):\n", len(schema.Fields()))
	for i, field := range schema.Fields() {
		fmt.Printf("  %d: %s (%s)\n", i, field.Name, field.Type)
	}
}
