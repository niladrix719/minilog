package harness

// ---------------------------------------------------------------------------
// STAGE 3 MEASUREMENTS -- COMPLETE.
//
//	go test ./harness -run TestStage3 -v
//
// Two things are being established here:
//   1. merges never change what a query returns (100 random schedules)
//   2. the write-amplification / read-amplification tradeoff, as a table you
//      produce from your own engine
//
// (2) is the most valuable output of the whole exercise. Save the table.
// ---------------------------------------------------------------------------

import (
	"math"
	"math/rand"
	"testing"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
)

// TestStage3MergeCorrectness runs many random merge schedules over the same
// data and asserts a full scan is identical every time.
//
// Random schedules matter. A merge bug that only appears when a 1-block part
// is merged into a 500-block part will never show up in a fixed schedule, and
// merge bugs are silent: they duplicate or drop rows without erroring.
func TestStage3MergeCorrectness(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 50_000
	cfg.Streams = 50
	ds := gen.Generate(cfg)

	want := make([]minilog.Row, len(ds.Rows))
	copy(want, ds.Rows)
	minilog.SortRows(want)

	fullScan := &minilog.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}

	const schedules = 100
	rng := rand.New(rand.NewSource(1234))

	for sched := 0; sched < schedules; sched++ {
		s := newStorage(t, &minilog.Config{ShardsCount: 1})

		// Ingest in randomly-sized batches, forcing a flush after each so the
		// part sizes vary wildly, then merge at random points.
		rows := ds.Rows
		for len(rows) > 0 {
			n := 1 + rng.Intn(8000)
			if n > len(rows) {
				n = len(rows)
			}
			s.MustAddRows(rows[:n])
			rows = rows[n:]

			if rng.Intn(4) == 0 {
				s.MustForceFlush()
			}
			if rng.Intn(10) == 0 {
				s.MustForceMerge()
			}
		}
		s.MustForceFlush()

		// Query before the final merge...
		gotBefore, _ := s.Search(fullScan)
		if ok, msg := rowsEqual(gotBefore, want); !ok {
			t.Fatalf("schedule %d: full scan wrong BEFORE final merge: %s", sched, msg)
		}

		// ...and after. These must be identical.
		s.MustForceMerge()
		gotAfter, _ := s.Search(fullScan)
		if ok, msg := rowsEqual(gotAfter, want); !ok {
			t.Fatalf("schedule %d: full scan wrong AFTER merge: %s\n"+
				"Classic causes:\n"+
				"  - duplicated rows: parts.json update was not atomic, so both the\n"+
				"    merged part and its sources were live at once\n"+
				"  - dropped rows: the k-way merge advanced two readers on a tie\n"+
				"  - reordered rows: the merge compares only streamID, not\n"+
				"    (streamID, timestamp)", sched, msg)
		}
	}
	t.Logf("MEASUREMENT %d random merge schedules, all produced identical full scans", schedules)
}

// TestStage3MergePreservesFilters checks merges do not corrupt bloom filters.
//
// A merge rebuilds every block, so it rebuilds every bloom filter. If the
// rebuild uses the wrong token set, selective queries silently start missing
// rows -- and a full scan would not catch it.
func TestStage3MergePreservesFilters(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	s := newStorage(t, &minilog.Config{ShardsCount: 1})
	ingest(t, s, ds, 3_000)

	q := &minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		Filters:      []minilog.Filter{{Column: "_msg", Token: gen.RareToken}},
	}
	want := bruteForce(ds.Rows, q)

	before, ssBefore := s.Search(q)
	if ok, msg := rowsEqual(before, want); !ok {
		t.Fatalf("before merge: %s", msg)
	}

	s.MustForceMerge()

	after, ssAfter := s.Search(q)
	if ok, msg := rowsEqual(after, want); !ok {
		t.Fatalf("after merge, the selective query lost rows: %s\n"+
			"The merge rebuilt blocks but the rebuilt bloom filters do not contain\n"+
			"the right tokens. Check that the merge path tokenizes the MERGED\n"+
			"values, not the source blocks' filters.", msg)
	}

	t.Logf("MEASUREMENT before merge: %s", ssBefore)
	t.Logf("MEASUREMENT after  merge: %s", ssAfter)
	t.Logf("MEASUREMENT blocks scanned %d -> %d after merge",
		ssBefore.BlocksScanned, ssAfter.BlocksScanned)
}

// TestStage3WriteAmplification produces the tradeoff table.
//
// It sweeps SmallPartsMergeThreshold and, for each value, reports:
//   - write amplification (bytes written / logical bytes)
//   - blocks scanned by a representative query (read amplification)
//
// Those two move in opposite directions. Seeing your own numbers do that is
// the point of stage 3.
func TestStage3WriteAmplification(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 300_000
	ds := gen.Generate(cfg)

	q := &minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		Filters:      []minilog.Filter{{Column: "level", Token: "error"}},
	}

	origThreshold := minilog.SmallPartsMergeThreshold
	defer func() { minilog.SmallPartsMergeThreshold = origThreshold }()

	type result struct {
		threshold   int
		writeAmp    float64
		compression float64
		blocksTotal int
		blocksScan  int
		partsMade   int64
		merges      int64
	}
	var results []result

	for _, threshold := range []int{2, 4, 8, 16, 64, 1 << 30} {
		minilog.SmallPartsMergeThreshold = threshold

		s := newStorage(t, &minilog.Config{ShardsCount: 1})
		ingest(t, s, ds, 5_000)

		_, ss := s.Search(q)

		results = append(results, result{
			threshold:   threshold,
			writeAmp:    minilog.WriteAmplification(),
			compression: minilog.CompressionRatio(),
			blocksTotal: ss.BlocksTotal,
			blocksScan:  ss.BlocksScanned,
			partsMade:   minilog.Metrics.PartsCreated.Load(),
			merges:      minilog.Metrics.MergesCount.Load(),
		})
		s.MustClose()
	}

	t.Logf("MEASUREMENT ==================== LSM TRADEOFF TABLE ====================")
	t.Logf("MEASUREMENT %10s %10s %8s %8s %12s %8s %8s",
		"threshold", "writeAmp", "compr", "merges", "partsMade", "blocks", "scanned")
	for _, r := range results {
		label := "never"
		if r.threshold < 1<<30 {
			label = itoa(r.threshold)
		}
		t.Logf("MEASUREMENT %10s %9.2fx %7.2fx %8d %12d %8d %8d",
			label, r.writeAmp, r.compression, r.merges, r.partsMade, r.blocksTotal, r.blocksScan)
	}
	t.Logf("MEASUREMENT ====================================================================")
	t.Logf("NOTE Read the two end rows against each other. threshold=2 merges\n" +
		"     aggressively: high write amplification, few blocks to scan.\n" +
		"     threshold=never does no merges at all: write amp ~1.0, many more\n" +
		"     blocks. Everything in between is the tuning curve every LSM engine\n" +
		"     exposes as a knob. You just derived it.")

	if len(results) < 2 {
		t.Fatal("sweep produced no results")
	}
	aggressive := results[0]
	never := results[len(results)-1]

	// NOTE: compares against the no-merge baseline rather than a fixed 1.0x.
	// This engine's on-disk compression (flate on string values) is strong
	// enough that even the no-merge baseline can sit well under 1.0x logical
	// bytes, so a fixed threshold does not hold across compression schemes.
	// What must always hold is the LSM invariant: merging rewrites data, so
	// aggressive merging must write strictly more than no merging at all,
	// relative to that engine's own baseline.
	if aggressive.writeAmp <= never.writeAmp {
		t.Errorf("no-merge write amp (%.2fx) is not below aggressive-merge write amp (%.2fx).\n"+
			"That inverts the fundamental LSM tradeoff -- either merges are not\n"+
			"happening/being counted, or something is miscounted.",
			never.writeAmp, aggressive.writeAmp)
	}
	if never.blocksTotal <= aggressive.blocksTotal {
		t.Errorf("no-merge run considered %d blocks, aggressive-merge run considered %d.\n"+
			"Not merging should leave MORE blocks around, not fewer.",
			never.blocksTotal, aggressive.blocksTotal)
	}
}

func itoa(n int) string {
	return sprintf("%d", n)
}
