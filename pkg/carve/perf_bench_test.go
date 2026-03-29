package carve

import (
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func benchmarkLines() [][]byte {
	return generateLogLinesAsBytes(8192, 1.0)
}

func BenchmarkScannerScan(b *testing.B) {
	scanner, _ := NewExtractor(benchmarkPattern)
	out := make([][]byte, len(scanner.fields))
	lines := benchmarkLines()
	bytesPerIter := int64(0)
	for _, line := range lines {
		bytesPerIter += int64(len(line))
	}

	b.ReportAllocs()
	b.SetBytes(bytesPerIter)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			scanner.Scan(line, out)
		}
	}
}

func BenchmarkWriterWriteLinesSIMD(b *testing.B) {
	ext, _ := NewExtractor(benchmarkPattern)
	scanner := ext.Scanner(Options{ZeroCopy: true})
	lines := benchmarkLines()
	bytesPerIter := int64(0)
	for _, line := range lines {
		bytesPerIter += int64(len(line))
	}

	b.ReportAllocs()
	b.SetBytes(bytesPerIter)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		writer := NewWriter(ext.Schema(), memory.DefaultAllocator, len(lines))
		rec, err := writer.WriteLinesSIMD(lines, scanner)
		if err != nil {
			b.Fatal(err)
		}
		if rec != nil {
			rec.Release()
		}
	}
}

func BenchmarkFlush(b *testing.B) {
	ext, _ := NewExtractor(benchmarkPattern)
	scanner := ext.Scanner(Options{ZeroCopy: true})
	lines := benchmarkLines()
	writer := NewWriter(ext.Schema(), memory.DefaultAllocator, len(lines))
	for _, line := range lines {
		scanner.Scan(line, writer.scratch)
		for i := range writer.scratch {
			v := writer.scratch[i]
			writer.builders[i].Append(v)
		}
	}
	writer.rows = len(lines)

	var mStart, mEnd runtime.MemStats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.ReadMemStats(&mStart)
		rec, err := writer.Flush()
		if err != nil {
			b.Fatal(err)
		}
		if rec != nil {
			rec.Release()
		}
		runtime.ReadMemStats(&mEnd)
		if i == 0 {
			b.ReportMetric(float64(mEnd.HeapAlloc-mStart.HeapAlloc), "heap_delta/op")
		}
		for _, line := range lines {
			scanner.Scan(line, writer.scratch)
			for i := range writer.scratch {
				v := writer.scratch[i]
				writer.builders[i].Append(v)
			}
		}
		writer.rows = len(lines)
	}
}

func BenchmarkEndToEndPipeline(b *testing.B) {
	ext, _ := NewExtractor(benchmarkPattern)
	scanner := ext.Scanner(Options{ZeroCopy: true})
	lines := benchmarkLines()
	bytesPerIter := int64(0)
	for _, line := range lines {
		bytesPerIter += int64(len(line))
	}

	var mStart, mEnd runtime.MemStats
	b.ReportAllocs()
	b.SetBytes(bytesPerIter)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runtime.ReadMemStats(&mStart)
		writer := NewWriter(ext.Schema(), memory.DefaultAllocator, len(lines))
		rec, err := writer.WriteLinesSIMD(lines, scanner)
		if err != nil {
			b.Fatal(err)
		}
		if rec == nil {
			rec, err = writer.Flush()
			if err != nil {
				b.Fatal(err)
			}
		}
		if rec != nil {
			rec.Release()
		}
		runtime.ReadMemStats(&mEnd)
		if i == 0 {
			b.ReportMetric(float64(mEnd.TotalAlloc-mStart.TotalAlloc)/8.192, "B/1k_rows")
		}
	}
}
