#!/usr/bin/env python3
"""ndjson_bench: the six Pison queries over NDJSON with DuckDB, Polars and
Apache Arrow (pyarrow), for comparison with jsonstreambench.

    bench.py --system duckdb|polars|arrow --dataset twitter --file twitter.ndjson
             --threads 8 [--reps 10]

One process measures one system, one dataset and one thread count: Polars
fixes its thread pool when it is imported, so the thread count must be set
before the import, and the other two are run the same way for uniformity.

The timed region goes from the input to the extracted values, materialized in
memory: a DuckDB result fetched as an Arrow table, a Polars data frame, or
Arrow arrays. Each system is given an explicit schema that names only the
fields the query reads, the setting most favourable to it (no schema
inference, no columns built for the other fields). Polars and Arrow read from
an in-memory buffer. DuckDB reads from a path; run.sh puts the files in
/dev/shm so that the read is a copy from memory.

After the timed runs, outside the timed region, the values are counted (null
values excluded) and summed as CRC-32s of their text, so that the three
systems can be checked against each other and the counts against
jsonstreambench's.
"""
import argparse
import os
import sys
import time
import zlib

DATASETS = ("twitter", "bestbuy", "google_map", "nspl", "walmart", "wiki")

# Match counts of jsonstreambench (Table 3 of the paper).
EXPECTED = {
    "twitter": 300270,
    "bestbuy": 459332,
    "google_map": 1716752,
    "nspl": 3509764,
    "walmart": 288391,
    "wiki": 15603,
}


# --------------------------------------------------------------------------
# DuckDB
# --------------------------------------------------------------------------
def duckdb_sql(dataset, path):
    def rj(columns):
        # records=true: without it DuckDB samples the input to detect the
        # layout, even with the columns given.
        return (f"read_json('{path}', format='newline_delimited', records=true, "
                f"columns={columns})")

    if dataset == "twitter":   # $.user.lang, $.lang
        return (f"SELECT lang, \"user\".lang AS user_lang FROM "
                f"{rj({'lang': 'VARCHAR', 'user': 'STRUCT(lang VARCHAR)'})}")
    if dataset == "bestbuy":   # $.categoryPath[1:3].id
        return (f"SELECT \"categoryPath\"[2].id AS id1, \"categoryPath\"[3].id AS id2 FROM "
                f"{rj({'categoryPath': 'STRUCT(id VARCHAR)[]'})}")
    if dataset == "google_map":  # $.routes[*].legs[*].steps[*].distance.text
        t = "STRUCT(legs STRUCT(steps STRUCT(distance STRUCT(text VARCHAR))[])[])[]"
        return ("SELECT unnest(flatten(flatten("
                "[[[s.distance.text FOR s IN l.steps] FOR l IN r.legs] FOR r IN routes]"
                f"))) AS text FROM {rj({'routes': t})}")
    if dataset == "nspl":      # $[8], $[9]: rows are positional arrays
        return (f"SELECT json_extract_string(json, '$[8]') AS c8, "
                f"json_extract_string(json, '$[9]') AS c9 "
                f"FROM read_ndjson_objects('{path}')")
    if dataset == "walmart":   # $.bestMarketplacePrice.price, $.name
        return (f"SELECT \"bestMarketplacePrice\".price AS price, name FROM "
                f"{rj({'bestMarketplacePrice': 'STRUCT(price DOUBLE)', 'name': 'VARCHAR'})}")
    if dataset == "wiki":      # $.claims.P150[*].mainsnak.property
        t = 'STRUCT("P150" STRUCT(mainsnak STRUCT(property VARCHAR))[])'
        return ("SELECT unnest([c.mainsnak.property FOR c IN claims.\"P150\"]) AS property "
                f"FROM {rj({'claims': t})}")
    raise ValueError(dataset)


def make_duckdb(dataset, path, data, threads):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    sql = duckdb_sql(dataset, path)

    def run():
        table = con.sql(sql).to_arrow_table()
        return [table.column(i) for i in range(table.num_columns)]

    return run, f"duckdb={duckdb.__version__}"


# --------------------------------------------------------------------------
# Polars
# --------------------------------------------------------------------------
def make_polars(dataset, path, data, threads):
    import io
    import polars as pl
    S, L = pl.Struct, pl.List

    if dataset == "nspl":
        # read_ndjson wants an object per line; NSPL rows are arrays. Read
        # each line as one string and apply Polars' JSONPath function.
        def run():
            lines = pl.read_csv(io.BytesIO(data), has_header=False, separator="\x01",
                                quote_char=None, new_columns=["line"],
                                schema={"line": pl.String})
            out = lines.select(pl.col("line").str.json_path_match("$[8]").alias("c8"),
                               pl.col("line").str.json_path_match("$[9]").alias("c9"))
            return [out[c] for c in out.columns]
        return run, f"polars={pl.__version__}"

    if dataset == "twitter":
        schema = {"lang": pl.String, "user": S({"lang": pl.String})}
        select = [pl.col("lang"), pl.col("user").struct.field("lang").alias("user_lang")]
    elif dataset == "bestbuy":
        schema = {"categoryPath": L(S({"id": pl.String}))}
        cp = pl.col("categoryPath")
        select = [cp.list.get(1, null_on_oob=True).struct.field("id").alias("id1"),
                  cp.list.get(2, null_on_oob=True).struct.field("id").alias("id2")]
    elif dataset == "google_map":
        schema = {"routes": L(S({"legs": L(S({"steps": L(S({
            "distance": S({"text": pl.String})}))}))}))}
        select = [pl.col("routes").explode().struct.field("legs").explode()
                  .struct.field("steps").explode().struct.field("distance")
                  .struct.field("text")]
    elif dataset == "walmart":
        schema = {"bestMarketplacePrice": S({"price": pl.Float64}), "name": pl.String}
        select = [pl.col("bestMarketplacePrice").struct.field("price"), pl.col("name")]
    elif dataset == "wiki":
        schema = {"claims": S({"P150": L(S({"mainsnak": S({"property": pl.String})}))})}
        select = [pl.col("claims").struct.field("P150").explode()
                  .struct.field("mainsnak").struct.field("property")]
    else:
        raise ValueError(dataset)

    def run():
        df = pl.read_ndjson(io.BytesIO(data), schema=schema).select(select)
        return [df[c] for c in df.columns]

    return run, f"polars={pl.__version__}"


# --------------------------------------------------------------------------
# Apache Arrow
# --------------------------------------------------------------------------
def make_arrow(dataset, path, data, threads):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.json as pj
    pa.set_cpu_count(threads)
    pa.set_io_thread_count(threads)
    S, L, s = pa.struct, pa.list_, pa.string()

    if dataset == "nspl":
        return None, f"pyarrow={pa.__version__}"  # reads objects only
    if dataset == "twitter":
        schema = pa.schema([("lang", s), ("user", S([("lang", s)]))])

        def extract(t):
            return [t["lang"], pc.struct_field(t["user"], "lang")]
    elif dataset == "bestbuy":
        schema = pa.schema([("categoryPath", L(S([("id", s)])))])

        def extract(t):
            sliced = pc.list_slice(t["categoryPath"], 1, 3)
            return [pc.struct_field(pc.list_flatten(sliced), "id")]
    elif dataset == "google_map":
        schema = pa.schema([("routes", L(S([("legs", L(S([("steps", L(S([
            ("distance", S([("text", s)]))])))])))])))])

        def extract(t):
            legs = pc.struct_field(pc.list_flatten(t["routes"]), "legs")
            steps = pc.struct_field(pc.list_flatten(legs), "steps")
            dist = pc.struct_field(pc.list_flatten(steps), "distance")
            return [pc.struct_field(dist, "text")]
    elif dataset == "walmart":
        schema = pa.schema([("bestMarketplacePrice", S([("price", pa.float64())])),
                            ("name", s)])

        def extract(t):
            return [pc.struct_field(t["bestMarketplacePrice"], "price"), t["name"]]
    elif dataset == "wiki":
        schema = pa.schema([("claims", S([("P150", L(S([
            ("mainsnak", S([("property", s)]))])))]))])

        def extract(t):
            p150 = pc.list_flatten(pc.struct_field(t["claims"], "P150"))
            return [pc.struct_field(pc.struct_field(p150, "mainsnak"), "property")]
    else:
        raise ValueError(dataset)

    read_options = pj.ReadOptions(use_threads=threads > 1)
    parse_options = pj.ParseOptions(explicit_schema=schema,
                                    unexpected_field_behavior="ignore")
    buf = pa.py_buffer(data)

    def run():
        table = pj.read_json(pa.BufferReader(buf), read_options=read_options,
                             parse_options=parse_options)
        return extract(table)

    return run, f"pyarrow={pa.__version__}"


# --------------------------------------------------------------------------
def check(columns):
    """Number of non-null values and the sum of CRC-32s of their text."""
    import pyarrow as pa
    count, total = 0, 0
    for col in columns:
        if not isinstance(col, (pa.Array, pa.ChunkedArray)):
            col = col.to_arrow()  # Polars series
        for v in col.to_pylist():
            if v is None:
                continue
            count += 1
            total += zlib.crc32((repr(v) if isinstance(v, float) else str(v)).encode())
    return count, total % (1 << 64)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--system", required=True, choices=("duckdb", "polars", "arrow"))
    ap.add_argument("--dataset", required=True, choices=DATASETS)
    ap.add_argument("--file", required=True)
    ap.add_argument("--threads", type=int, required=True)
    ap.add_argument("--reps", type=int, default=10)
    a = ap.parse_args()

    if a.system == "polars" and "polars" in sys.modules:
        sys.exit("polars imported before POLARS_MAX_THREADS was set")
    os.environ["POLARS_MAX_THREADS"] = str(a.threads)

    with open(a.file, "rb") as f:
        data = f.read()
    make = {"duckdb": make_duckdb, "polars": make_polars, "arrow": make_arrow}[a.system]
    run, version = make(a.dataset, a.file, data, a.threads)
    if run is None:
        print(f"SKIP system={a.system} dataset={a.dataset} threads={a.threads} "
              f"reason=unsupported {version}", flush=True)
        return

    columns = run()  # untimed warm-up
    times = []
    for _ in range(a.reps):
        start = time.perf_counter()
        columns = run()
        times.append(time.perf_counter() - start)
    count, checksum = check(columns)
    best = min(times)
    print(f"RESULT system={a.system} dataset={a.dataset} threads={a.threads} "
          f"bytes={len(data)} best_s={best:.6f} gibps={len(data) / best / 2**30:.3f} "
          f"spread_pct={100 * (max(times) - best) / best:.1f} matches={count} "
          f"expected={EXPECTED[a.dataset]} checksum={checksum} reps={a.reps} {version}",
          flush=True)


if __name__ == "__main__":
    main()
