# minilog

A miniature log storage engine, built to understand [VictoriaLogs](https://github.com/VictoriaMetrics/VictoriaLogs).

It makes the same design bets as `lib/logstorage`: immutable columnar parts,
an LSM merge tree, per-block bloom filters instead of an inverted index, and
day-granularity partitions. When it works, reading the real repo feels like
reading your own code with more features.

**You implement it.** Everything marked `TODO stage N` is yours. The generator
and every measurement are complete and must not be weakened.

---

## The rule

> Every stage ships with a measurement that can **fail against an independent
> prediction** — closed-form theory, or a brute-force scan that shares no code
> with the thing it checks.

A test that asserts what you coded proves nothing. A bloom filter whose
measured false-positive rate matches `(1 - e^(-k/bitsPerItem))^k` cannot be a
coincidence.

---

## Getting started

```sh
make            # list the stages
make stage1     # fails immediately with: panic: TODO stage 1: implement MarshalVarUint64
```

The panic names the exact file and function to write next. Work down the
list. When stage 1 goes green, move to stage 2.

---

## Stages

| Stage | Files you write | Verified by |
|---|---|---|
| **1** — parts & on-disk format | `encoding.go` `block.go` `part_writer.go` `part_reader.go` `search.go` | `make stage1` |
| **2** — bloom filters & tokens | `bloom.go` `tokenizer.go` + bloom checks in `part_reader.go` | `make stage2` |
| **3** — the LSM | `merger.go` `partition.go` `storage.go` | `make stage3`, `make crash` |
| **4** — partitions & sharding | day routing + shards in `storage.go` | `make stage4`, `make race` |
| **5** — compare to the real thing | — | see below |

### Stage 1 — immutable parts

Rows → sorted blocks → real binary files. No LSM, no bloom yet.

```
part-000001/
  metadata.json     rowsCount, min/maxTimestamp, sizeBytes
  metaindex.bin     [indexBlockHeader...]   small, read fully at open
  index.bin         [blockHeader...]        offsets into the files below
  timestamps.bin    delta-encoded, per block
  values.bin        per column, per block, typed encoding
  bloom.bin         stage 2
```

Binary, not JSON — hand-rolled varints. Parts are immutable once closed.

**Measures:** exact round-trip of 200k rows through real files, and your
compression ratio versus raw JSON. Target ≥ 5x. The test tells you which
lever to pull if you miss it.

### Stage 2 — bloom filters

One filter per `(block, column)`, using VictoriaLogs' exact parameters:
**k = 6 hashes, 16 bits per item, xxhash**.

**Measures:**
- **No false negatives.** Non-negotiable: a "no" on a present token silently
  drops log lines from results.
- **False-positive rate vs. theory.** For k=6, bits/item=16 the prediction is
  **≈ 0.094%**. The harness computes it from your exported constants and tells
  you what the *direction* of any error means — too low usually means you
  sized from a non-deduped token count; too high usually means your k hashes
  are correlated (use Kirsch-Mitzenmacher double hashing, not `xxhash(token+i)`).
- **Skip ratio.** A rare token must eliminate >95% of blocks; a token present
  in every row must eliminate ~0%. The gap between those two numbers is the
  entire argument for not maintaining an inverted index.

### Stage 3 — the LSM

Three tiers — in-memory → small file parts → big file parts — with a k-way
streaming merge and a `parts.json` manifest updated by atomic rename.

**Measures:**
- **Merge correctness.** 100 random merge schedules over the same data; a full
  scan must be identical every time. Random schedules matter — merge bugs are
  silent and schedule-dependent.
- **The LSM tradeoff table.** The harness sweeps `SmallPartsMergeThreshold`
  and prints write amplification against blocks-scanned-per-query. They move
  in opposite directions. **This table is the most valuable thing you will
  produce in this exercise** — save it.
- **Crash safety.** `make crash` SIGKILLs an ingesting child process at 200
  random moments and asserts every acknowledged row survives, nothing is
  duplicated, and reopening never panics. If you cannot survive 200 random
  kills you do not have a storage engine.

### Stage 4 — partitions & sharding

Day-granularity partition directories; retention as `os.RemoveAll`; shards
hashed by StreamID and searched in parallel.

**Measures:** a 1-hour query against 90 days must open **1** partition, not 90.
Retention timing. A shard-count sweep (1/2/4/8) with latency — find where the
speedup stops and work out why. Run `make race` too.

### Stage 5 — close the loop

1. Generate comparable datasets for minilog and real VictoriaLogs
   (`./bin/vlogsgenerator`).
2. Compare bytes on disk, ingest throughput, query latency.
3. You will lose. **By how much, and on which axis?**
4. Take the biggest gap, open the corresponding file in `lib/logstorage`, and
   find what they do that you don't.

Things people usually find: `sync.Pool` removing per-row allocation,
`bloomValuesMaxShardsCount = 128` enabling parallel reads, const-column
extraction, `_msg` in its own file, zstd.

Every optimisation in the real repo now has a number attached that *you*
measured on *your* code.

---

## Map to the real repo

| minilog | VictoriaLogs |
|---|---|
| `minilog/encoding.go` | `lib/logstorage/values_encoder.go`, `encoding.go` |
| `minilog/bloom.go` | `lib/logstorage/bloomfilter.go` |
| `minilog/tokenizer.go` | `lib/logstorage/tokenizer.go` |
| `minilog/block.go` | `lib/logstorage/block.go`, `block_header.go` |
| `minilog/part_writer.go` | `lib/logstorage/block_stream_writer.go` |
| `minilog/part_reader.go` | `lib/logstorage/part.go`, `block_search.go` |
| `minilog/merger.go` | `lib/logstorage/block_stream_merger.go`, `datadb.go` |
| `minilog/partition.go` | `lib/logstorage/partition.go`, `datadb.go` |
| `minilog/storage.go` | `lib/logstorage/storage.go`, `storage_search.go` |

---

## Deliberately out of scope

No query language, no pipes, no aggregations, no cluster mode, no HTTP API,
no deletes, no WAL. The exercise is storage + bloom + LSM + partitioning.
Adding LogsQL doubles the work and teaches nothing new about any of them.

## Notes to keep

The stub comments ask you to record decisions in place — tokenizer casing,
whether dict columns need a bloom at all, what a WAL would change. Answer them
in the code as you go. They are the parts you will otherwise forget you
decided.
