# Carve

`carve` is a command line tool and Go library for extracting structured data from text using regular expressions with named capture groups. It streams input line-by-line, applies a regex, and emits an [Apache Arrow](https://arrow.apache.org/) IPC file.

## Building

```
# requires Go 1.20+
go build ./cmd/carve
```

## Usage

```
carve --pattern '(?P<ts>[^ ]+) (?P<level>[A-Z]+) (?P<msg>.+)' \
      --input /var/log/app.log \
      --output logs.arrow
```

The resulting `.arrow` file can be queried with DuckDB or other analytics engines.
