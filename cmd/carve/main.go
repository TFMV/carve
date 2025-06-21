package main

import (
    "bufio"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "regexp"

    "github.com/apache/arrow/go/v18/arrow"
    "github.com/apache/arrow/go/v18/arrow/array"
    "github.com/apache/arrow/go/v18/arrow/ipc"
    "github.com/apache/arrow/go/v18/arrow/memory"
)

func main() {
    pattern := flag.String("pattern", "", "regex pattern with named capture groups")
    input := flag.String("input", "", "input file (defaults to stdin)")
    output := flag.String("output", "", "output Arrow IPC file")
    maxRows := flag.Int("max-rows", 0, "maximum rows to process (0 for no limit)")
    flag.Parse()

    if *pattern == "" || *output == "" {
        flag.Usage()
        os.Exit(1)
    }

    re, err := regexp.Compile(*pattern)
    if err != nil {
        log.Fatalf("failed to compile pattern: %v", err)
    }

    subexpNames := re.SubexpNames()
    var groupIdx []int
    var fields []arrow.Field
    for i, name := range subexpNames {
        if name == "" {
            continue
        }
        groupIdx = append(groupIdx, i)
        fields = append(fields, arrow.Field{Name: name, Type: arrow.BinaryTypes.String})
    }
    if len(fields) == 0 {
        log.Fatalf("pattern must contain at least one named capture group")
    }

    schema := arrow.NewSchema(fields, nil)
    mem := memory.NewCheckedAllocator(memory.DefaultAllocator)

    builders := make([]*array.StringBuilder, len(fields))
    for i := range builders {
        builders[i] = array.NewStringBuilder(mem)
    }

    var r io.Reader = os.Stdin
    if *input != "" {
        f, err := os.Open(*input)
        if err != nil {
            log.Fatalf("failed to open input: %v", err)
        }
        defer f.Close()
        r = f
    }

    scanner := bufio.NewScanner(r)
    rowCount := 0
    for scanner.Scan() {
        line := scanner.Text()
        matches := re.FindStringSubmatch(line)
        if matches == nil {
            continue
        }
        for i, idx := range groupIdx {
            builders[i].Append(matches[idx])
        }
        rowCount++
        if *maxRows > 0 && rowCount >= *maxRows {
            break
        }
    }
    if err := scanner.Err(); err != nil {
        log.Fatalf("scan error: %v", err)
    }

    arrays := make([]arrow.Array, len(builders))
    for i, b := range builders {
        arrays[i] = b.NewArray()
        defer arrays[i].Release()
        b.Release()
    }

    table := array.NewTable(schema, arrays, int64(rowCount))
    defer table.Release()

    outFile, err := os.Create(*output)
    if err != nil {
        log.Fatalf("failed to create output: %v", err)
    }
    defer outFile.Close()

    writer := ipc.NewFileWriter(outFile, ipc.WithSchema(schema))
    if err := writer.Write(table); err != nil {
        log.Fatalf("failed to write table: %v", err)
    }
    writer.Close()

    fmt.Printf("wrote %d rows to %s\n", rowCount, *output)
}

