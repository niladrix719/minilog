package minilog

// ---------------------------------------------------------------------------
// STAGE 3 -- YOUR IMPLEMENTATION GOES HERE.
//
// Verified by:  go test ./harness -run TestStage3 -v
//               go run ./cmd/crashtest
//
// Reference: lib/logstorage/block_stream_merger.go, datadb.go.
// ---------------------------------------------------------------------------

// blockStreamReader iterates a part's blocks in (streamID, minTimestamp)
// order. It is the input to the k-way merge.
//
// Model it on an iterator with NextBlock() bool + Block() *block, because
// that is what a heap wants. Do not materialise all blocks: a merge of eight
// big parts must run in memory proportional to the number of parts, not to
// their size. That constraint is the reason merges are streaming in the first
// place, and skipping it is the classic way a toy LSM stops being real.
type blockStreamReader struct {
	// TODO stage 3
}

// blockStreamMerger performs a k-way merge over several blockStreamReaders.
//
// The merge is on (streamID, timestamp). Use container/heap over the readers.
//
// Subtlety worth thinking about before you write it: the inputs are streams of
// BLOCKS, but the merge order is defined on ROWS. Two blocks from different
// parts can overlap in time for the same stream. You have three options:
//
//	a) decode everything to rows, merge rows, re-block. Simple, correct, slow.
//	b) pass through blocks that do not overlap anything, decode only the ones
//	   that do. Much faster on the common case (parts usually cover disjoint
//	   time ranges) and meaningfully harder.
//	c) something in between.
//
// Start with (a) and get the correctness harness green. Then implement (b)
// and measure the merge throughput difference on the same dataset. Write the
// two numbers down. VictoriaLogs does (b), and knowing what it buys is worth
// more than having done it.
type blockStreamMerger struct {
	// TODO stage 3
}

// mustMergeParts merges srcParts into a single new part at dstPath.
//
// Contract:
//   - the output contains exactly the union of rows in srcParts (no dedup;
//     this store has no notion of row identity)
//   - output blocks are in ascending (streamID, minTimestamp) order
//   - srcParts are NOT deleted here. The caller deletes them, and only after
//     parts.json has been updated to point at the new part. Deleting them
//     here would make a crash mid-merge lose data.
//
// The correctness harness runs 100 random merge schedules over the same data
// and asserts a full scan is identical every time. Random schedules matter:
// a merge bug that only shows up when merging a 1-block part into a 500-block
// part will not appear in a fixed schedule.
func mustMergeParts(dstPath string, srcParts []*part) *partMetadata {
	var rows []Row
	var blocks []block
	for _, part := range srcParts {
		rows = append(rows, part.mustReadAllRows()...)
	}
	SortRows(rows)
	i := 0
	for i < len(rows) {
		j := i + 1
		for j < len(rows) && rows[i].StreamID == rows[j].StreamID && j-i < MaxRowsPerBlock {
			j++
		}
		var b block
		b.mustInitFromRows(rows[i:j])
		blocks = append(blocks, b)
		i = j
	}
	pw := mustCreatePartWriter(dstPath)
	for _, b := range blocks {
		pw.MustWriteBlock(&b)
	}
	pw.MustClose()
	return &pw.md
}

// partsToMerge selects which parts to merge next, or returns nil.
//
// The simplest workable policy, and the one to start with: if there are at
// least SmallPartsMergeThreshold parts, merge the smallest ones.
//
// Once it works, this function is the knob the write-amplification harness
// turns. Things to try and measure:
//   - always merge everything (write amp explodes, query cost is minimal)
//   - never merge (write amp = 1, query cost explodes)
//   - size-tiered: only merge parts within 2x of each other
//   - levelled: RocksDB-style, each level 10x the last
//
// The harness plots write-amp against blocks-scanned-per-query for whatever
// policy you implement. Three policies on one plot is the deliverable for
// stage 3, and it is the single most valuable artifact in this exercise --
// you will have derived the LSM tuning tradeoff from your own measurements
// rather than read about it.
func partsToMerge(parts []*part) []*part {
	if len(parts) < SmallPartsMergeThreshold {
		return nil
	}
	return parts
}
