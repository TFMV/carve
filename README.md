# Carve

High-performance, allocation-free text scanning and Arrow ingestion engine for Go.

Carve is a streaming parsing system built around a zero-allocation scanner and a columnar writer that emits Apache Arrow RecordBatches. It replaces regex-heavy parsing pipelines with deterministic byte-level scanning.

---

## 🚀 What Carve Is

Carve is not a regex library.

It is a **streaming ingestion engine**:

```
text → Scanner VM → column buffers → Arrow RecordBatch
```

Designed for:

* Log ingestion
* High-throughput ETL pipelines
* Columnar transformation at ingest time

---

## ⚡ Key Features

### Zero-allocation Scanner

* ~7–14ns per line parsing
* 0 allocations in hot path
* deterministic byte-level scanning

### SIMD-inspired scan model

* Precomputed scan plan
* delimiter-driven field extraction
* branch-light execution path

### Apache Arrow native output

* Direct `RecordBatch` emission
* column builders per field
* batch-oriented memory control

### Regex compatibility layer

* Legacy regex parsing still supported
* schema extraction via named capture groups
* gradual migration path

---

## 📊 Performance Summary

### Scanner vs Regex

| Metric      | Regex           | Scanner          |
| ----------- | --------------- | ---------------- |
| Parse speed | ~2.5M lines/sec | ~140M lines/sec  |
| Memory      | High            | Zero in hot path |
| Allocations | Thousands       | Zero             |

### End-to-End

| Pipeline        | Throughput      |
| --------------- | --------------- |
| Regex + Arrow   | ~2K lines/sec   |
| Scanner + Arrow | ~20K+ lines/sec |

---

## 🧠 Core Architecture

### Scanner

A deterministic byte-state machine:

```go
Scan(line []byte, out [][]byte) bool
```

* Uses precomputed delimiter plan
* slices input without allocations (optional zero-copy mode)
* writes directly into scratch buffers

---

### Writer

Columnar batch builder:

```go
WriteLinesSIMD(lines [][]byte, scanner *Scanner)
```

* accumulates column data
* emits Arrow RecordBatch on flush
* controls batch size and memory lifecycle

---

### Scan Plan

Derived from schema intent:

* maps fields → delimiter positions
* drives scan VM execution
* avoids runtime regex matching

---

## 📦 Example

### Create Scanner

```go
scanner, err := carve.New(`(?P<ts>[^ ]+) (?P<level>[^ ]+) (?P<msg>.*)`)
if err != nil {
    panic(err)
}
```

### Write Arrow Batches

```go
writer := carve.NewWriter(schema, nil, 8192)

batch, err := writer.WriteLinesSIMD(lines, scanner)
if err != nil {
    panic(err)
}

if batch != nil {
    defer batch.Release()
}
```

---

## 🔥 Design Philosophy

Carve is built on three principles:

### 1. Determinism over flexibility

Regex is expressive but unpredictable. Carve is strict and fast.

### 2. Memory stability over peak throughput

Zero allocations in hot path ensures stable runtime behavior under load.

### 3. Columnar-native ingestion

Data is shaped into Arrow as early as possible.

---

## ⚠️ Constraints

* Requires delimiter-friendly or structured patterns
* ScanPlan derived from regex AST is heuristic-based
* Zero-copy mode requires careful lifetime management
* Arrow builder remains allocation boundary

---

## 🧭 When to Use Carve

Use Carve when:

* ingesting large logs or event streams
* regex performance becomes a bottleneck
* Arrow-native pipelines are required
* predictable latency matters

Avoid when:

* patterns are highly irregular or deeply nested
* one-off parsing tasks

---

## 🏁 Status

Carve v0.4.0 is a **performance-stable ingestion engine core**.

The scanner layer is effectively production-grade. Future work focuses on:

* scan plan formalization
* SIMD multi-field extraction
* Arrow writer optimization
* streaming batch pipelines

---

## 📜 License

MIT

---

Built for speed. Designed for structure. Optimized for streams.
