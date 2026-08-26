package harness

// ---------------------------------------------------------------------------
// STAGE 4 MEASUREMENTS -- COMPLETE.
//
//	go test ./harness -run TestStage4 -v
//	go test ./harness -run TestStage4 -race    <- run this one too
//
// Partition pruning, retention, and sharding.
// ---------------------------------------------------------------------------

import (
	"math"
	"testing"
	"time"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
)

const nsPerDay = int64(24 * 3600 * 1e9)

// generate90Days builds a dataset spanning 90 days.
func generate90Days(rows int) *gen.Dataset {
	cfg := gen.DefaultConfig()
	cfg.Rows = rows
	cfg.Streams = 100
	cfg.SpanNanos = 90 * nsPerDay
	return gen.Generate(cfg)
}

// TestStage4PartitionPruning is the headline stage 4 measurement.
//
// A 1-hour query against 90 days of data must open one partition, not ninety.
// This is the cheapest and highest-leverage skip in the entire system, and it
// costs nothing at write time -- it falls out of naming directories by day.
func TestStage4PartitionPruning(t *testing.T) {
	ds := generate90Days(900_000)

	s := newStorage(t, &minilog.Config{ShardsCount: 1})
	ingest(t, s, ds, 10_000)

	days := s.PartitionDays()
	t.Logf("MEASUREMENT partitions created: %d (dataset spans 90 days)", len(days))
	if len(days) < 80 {
		t.Fatalf("expected ~90 daily partitions, got %d -- is dayForTimestamp being applied?", len(days))
	}

	// Query a 1-hour window in the middle of the range.
	mid := days[len(days)/2]
	start := mid*nsPerDay + 6*3600*1e9
	q := &minilog.Query{MinTimestamp: start, MaxTimestamp: start + 3600*1e9}

	got, ss := s.Search(q)
	want := bruteForce(ds.Rows, q)
	if ok, msg := rowsEqual(got, want); !ok {
		t.Fatalf("1-hour query disagrees with brute force: %s", msg)
	}

	t.Logf("MEASUREMENT 1-hour query over 90 days: %s", ss)
	t.Logf("MEASUREMENT   partitions opened: %d / %d", ss.PartitionsOpened, ss.PartitionsTotal)

	if ss.PartitionsTotal < 80 {
		t.Fatalf("PartitionsTotal is %d -- you are not bumping the counter for every partition considered", ss.PartitionsTotal)
	}
	if ss.PartitionsOpened > 2 {
		t.Errorf("a 1-hour query opened %d partitions, expected 1 (2 at a day boundary).\n"+
			"Storage.Search is not pruning partitions by time range before descending\n"+
			"into them. This is a four-line check and it is worth more than every\n"+
			"other optimisation in the read path combined.", ss.PartitionsOpened)
	}

	// A full-range query must open everything -- proves pruning is not just
	// "always return 1".
	qAll := &minilog.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}
	_, ssAll := s.Search(qAll)
	if ssAll.PartitionsOpened != ssAll.PartitionsTotal {
		t.Errorf("full-range query opened %d/%d partitions -- pruning is dropping data",
			ssAll.PartitionsOpened, ssAll.PartitionsTotal)
	}
	t.Logf("MEASUREMENT ===> pruning avoided %.0f%% of partitions on the selective query",
		(1-float64(ss.PartitionsOpened)/float64(ss.PartitionsTotal))*100)
}

// TestStage4Retention checks that dropping old data is a directory removal.
func TestStage4Retention(t *testing.T) {
	ds := generate90Days(300_000)

	s := newStorage(t, &minilog.Config{ShardsCount: 1})
	ingest(t, s, ds, 10_000)

	before := s.PartitionDays()
	if len(before) < 80 {
		t.Fatalf("expected ~90 partitions, got %d", len(before))
	}

	cutoff := before[len(before)/2]

	start := time.Now()
	s.MustDropPartitionsBefore(cutoff)
	elapsed := time.Since(start)

	after := s.PartitionDays()
	for _, d := range after {
		if d < cutoff {
			t.Fatalf("partition for day %d survived a drop with cutoff %d", d, cutoff)
		}
	}

	dropped := len(before) - len(after)
	t.Logf("MEASUREMENT dropped %d partitions (~%d rows) in %v",
		dropped, len(ds.Rows)/2, elapsed)
	t.Logf("NOTE Compare that to what `DELETE FROM logs WHERE ts < ?` costs in a\n" +
		"     row store with a B-tree index: a scan, a per-row index update, and\n" +
		"     a vacuum. Time-partitioning turns retention into unlink().")

	// Data after the cutoff must be intact.
	q := &minilog.Query{MinTimestamp: cutoff * nsPerDay, MaxTimestamp: math.MaxInt64}
	got, _ := s.Search(q)
	want := bruteForce(ds.Rows, q)
	if ok, msg := rowsEqual(got, want); !ok {
		t.Fatalf("retention damaged surviving data: %s", msg)
	}
}

// TestStage4Sharding sweeps shard counts and reports query latency.
//
// Run with -race as well: this is the test most likely to expose shared
// mutable state on *part or a SearchStats being written from several
// goroutines.
func TestStage4Sharding(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 500_000
	cfg.Streams = 2000
	ds := gen.Generate(cfg)

	q := &minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		Filters:      []minilog.Filter{{Column: "_msg", Token: gen.RareToken}},
	}
	want := bruteForce(ds.Rows, q)

	t.Logf("MEASUREMENT ============ SHARDING SWEEP ============")
	var base time.Duration
	for _, shards := range []int{1, 2, 4, 8} {
		s := newStorage(t, &minilog.Config{ShardsCount: shards})
		ingest(t, s, ds, 10_000)

		// warm
		s.Search(q)

		start := time.Now()
		const iters = 5
		var ss *minilog.SearchStats
		var got []minilog.Row
		for i := 0; i < iters; i++ {
			got, ss = s.Search(q)
		}
		elapsed := time.Since(start) / iters

		if ok, msg := rowsEqual(got, want); !ok {
			t.Fatalf("shards=%d: wrong results: %s\n"+
				"Sharding must not change what a query returns. Check that you merge\n"+
				"per-shard results and re-sort by (StreamID, Timestamp).", shards, msg)
		}

		if shards == 1 {
			base = elapsed
		}
		t.Logf("MEASUREMENT shards=%d  latency=%-12v speedup=%.2fx  blocks=%d scanned=%d",
			shards, elapsed, float64(base)/float64(elapsed), ss.BlocksTotal, ss.BlocksScanned)
		s.MustClose()
	}
	t.Logf("MEASUREMENT ==========================================")
	t.Logf("NOTE Find where the speedup stops. Then work out why: per-shard fixed\n" +
		"     costs, result-merge overhead, or memory bandwidth. Then try sharding\n" +
		"     round-robin instead of by StreamID and re-run -- a stream-filtered\n" +
		"     query should get dramatically worse, because it can no longer be\n" +
		"     answered by a single shard.")
}

// TestStage4StreamFilterHitsOneShard checks the point of hashing by StreamID.
func TestStage4StreamFilterHitsOneShard(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	cfg.Streams = 500
	ds := gen.Generate(cfg)

	s := newStorage(t, &minilog.Config{ShardsCount: 8})
	ingest(t, s, ds, 10_000)

	streamID := ds.Streams[0].ID
	q := &minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		StreamID:     &streamID,
	}

	got, ss := s.Search(q)
	want := bruteForce(ds.Rows, q)
	if ok, msg := rowsEqual(got, want); !ok {
		t.Fatalf("stream-filtered query disagrees with brute force: %s", msg)
	}

	t.Logf("MEASUREMENT stream-filtered query over 8 shards: %s", ss)
	t.Logf("NOTE Because rows are sharded by StreamID, this query only has data in\n" +
		"     one shard. Whether your implementation KNOWS that -- and skips the\n" +
		"     other seven without opening them -- is the difference between\n" +
		"     sharding as a parallelism trick and sharding as a pruning mechanism.")
}
