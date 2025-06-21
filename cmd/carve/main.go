package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"

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
	flag.Parse()

	if *showVersion {
		fmt.Println("carve", version)
		return
	}
	if *pattern == "" {
		log.Fatal("--pattern flag is required")
	}
	if *output == "" && !*schemaOnly {
		log.Fatal("--output flag is required")
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
		fmt.Println(schema)
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

	scanner := bufio.NewScanner(r)
	rows := 0
	for scanner.Scan() {
		line := scanner.Text()
		vals := carve.ParseLine(line, re)
		if vals == nil {
			if *verbose {
				log.Printf("[warn] line %d: does not match pattern", rows+1)
			}
			continue
		}
		writer.Append(vals)
		if writer.ShouldFlush() {
			rec := writer.Flush()
			err := writeRecord(*output, schema, rec, rows == 0)
			rec.Release()
			if err != nil {
				log.Fatalf("write error: %v", err)
			}
		}
		rows++
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("scan error: %v", err)
	}
	if writer.Rows() > 0 {
		rec := writer.Flush()
		err := writeRecord(*output, schema, rec, rows == 0)
		rec.Release()
		if err != nil {
			log.Fatalf("write error: %v", err)
		}
	}
	fmt.Printf("wrote %d rows to %s\n", rows, *output)
}

func writeRecord(path string, schema *arrow.Schema, rec arrow.Record, newFile bool) error {
	var f *os.File
	var err error
	if newFile {
		f, err = os.Create(path)
	} else {
		f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	}
	if err != nil {
		return err
	}
	defer f.Close()
	w := ipc.NewFileWriter(f, ipc.WithSchema(schema))
	if err := w.Write(rec); err != nil {
		return err
	}
	return w.Close()
}
