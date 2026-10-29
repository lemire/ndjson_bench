# ndjson_bench

The six JSONPath queries of the Pison benchmark, over the same NDJSON
(JSON-lines) corpora, with three data systems that read NDJSON in parallel:

* [DuckDB](https://duckdb.org) (`read_json`, which parses with yyjson);
* [Polars](https://pola.rs) (`read_ndjson`, which parses with simd-json);
* [Apache Arrow](https://arrow.apache.org) (`pyarrow.json.read_json`, which
  parses with RapidJSON).

It is a companion to [jsonstreambench](https://github.com/simdjson/jsonstreambench),
which measures simdjson's parallel stream parser on the same corpora and
queries, and lets the two be compared.

## Queries

| Dataset | Query | Matches |
|---|---|---:|
| twitter | `$.user.lang`, `$.lang` | 300,270 |
| bestbuy | `$.categoryPath[1:3].id` | 459,332 |
| google_map | `$.routes[*].legs[*].steps[*].distance.text` | 1,716,752 |
| nspl | `$[8]`, `$[9]` | 3,509,764 |
| walmart | `$.bestMarketplacePrice.price`, `$.name` | 288,391 |
| wiki | `$.claims.P150[*].mainsnak.property` | 15,603 |

Each system answers each query in its own idiom (`bench.py` has the code):

* every system gets an explicit schema naming only the fields the query reads,
  so there is no schema inference and no column is built for the other fields;
* DuckDB reads with `records=true`, without which it samples the input to
  detect its layout;
* NSPL records are positional arrays, which `read_ndjson` and
  `pyarrow.json` do not accept. DuckDB uses `read_ndjson_objects` and
  `json_extract_string`, Polars reads each line as a string and applies
  `str.json_path_match`, and Arrow is skipped.

## What is timed

From the input to the extracted values, materialized in memory: a DuckDB
result fetched as an Arrow table, a Polars data frame, or Arrow arrays. Polars
and Arrow read from an in-memory buffer; DuckDB reads from a path, so
`run.sh` copies the corpora to `/dev/shm` first. Each configuration runs once
untimed, then `--reps` times (default 10); the output gives the best time.

After timing, the values are counted (nulls excluded) and summed as CRC-32s
of their text. The three systems must agree on both, and the counts must
equal jsonstreambench's (the `expected` field).

One process measures one system, dataset and thread count: Polars sizes its
thread pool when it is imported (`POLARS_MAX_THREADS`), and the other two
(`SET threads`, `pyarrow.set_cpu_count`) are run the same way.

## Running

```sh
uv venv .venv
uv pip install --python .venv/bin/python duckdb polars pyarrow
./run.sh --data ~/jsonbench/ndjson --out results/<host>.txt
```

The corpora are the `<dataset>.ndjson` files that jsonstreambench's
`datasets.sh` builds.

## Results

`results/big4.txt`: a dual-socket Intel Xeon Gold 6548N (2 x 32 cores, 128
threads), unpinned, DuckDB 2.0.0, Polars 2.1.0 and pyarrow 26.0.0 under
Python 3.14. The header reads `host=localhost` because that is what the
machine's `hostname` reports.

Peak throughput over the thread sweep (GiB/s, threads in parentheses):

| | TT | BB | GMD | NSPL | WM | WP |
|---|---|---|---|---|---|---|
| DuckDB | 11.6 (32) | 11.7 (32) | 11.3 (64) | 5.35 (32) | 16.9 (32) | 12.2 (32) |
| Polars | 3.51 (16) | 3.67 (16) | 2.17 (64) | 5.32 (128) | 3.15 (8) | 3.24 (128) |
| Arrow | 24.6 (128) | 16.8 (128) | 3.23 (16) | -- | 29.1 (128) | 20.7 (128) |

On the same machine, jsonstreambench's parallel simdjson parser reaches 64 to
105 GiB/s on the same queries with the values decoded. Polars is fastest when
given an in-memory `BytesIO`, as here: passing `bytes`, a path in `/dev/shm`,
or `scan_ndjson` measured 2.3 to 3.0 GiB/s against 3.2 to 3.3 on the tweets.
