package harness

// ---------------------------------------------------------------------------
// STAGE 7 MEASUREMENTS -- COMPLETE.
//
//	go test ./harness -run TestStage7 -v
//	go test ./harness -run TestStage7 -race    <- run this one too
//
// Where does a row go, and when does it actually leave this process.
//
// The routing tradeoff table produced by TestStage7RoutingTradeoff is stage
// 7's equivalent of the stage 3 write-amplification table. Save it.
// ---------------------------------------------------------------------------

import (
	"math"
	"testing"
	"time"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
	"github.com/niladrix719/minilog/netinsert"
)

// ingestVia pushes a dataset through a netinsert client and flushes both the
// client buffers and every node.
func ingestVia(t *testing.T, ins *netinsert.Storage, ns *nodeSet, ds *gen.Dataset, batch int) {
	t.Helper()
	rows := ds.Rows
	for len(rows) > 0 {
		n := batch
		if n > len(rows) {
			n = len(rows)
		}
		ins.MustAddRows(rows[:n])
		rows = rows[n:]
	}
	ins.MustForceFlush()
	ns.flushAll()
}

// TestStage7NoRowsLost is the precondition for every other stage 7 number.
//
// Routing can be as unbalanced as it likes. It may not lose a row, duplicate a
// row, or put a row somewhere it cannot be found.
func TestStage7NoRowsLost(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	ds := gen.Generate(cfg)

	for _, policy := range []netinsert.RoutingPolicy{
		netinsert.RoutingHybrid, netinsert.RoutingSticky, netinsert.RoutingSpray,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			ns := newNodeSet(t, 4)
			ins := ns.insertClient(netinsert.Config{Routing: policy})
			ingestVia(t, ins, ns, ds, 5_000)

			total := 0
			for _, c := range ns.rowCounts() {
				total += c
			}
			if total != len(ds.Rows) {
				t.Fatalf("ingested %d rows, %d landed on nodes (%v).\n"+
					"Missing rows almost always mean MustForceFlush published the nodes\n"+
					"BEFORE draining this process's send buffers. Drain first, then\n"+
					"flush -- see the ordering note on netinsert.Storage.MustForceFlush.\n"+
					"More rows than ingested means a buffer was sent twice: check that\n"+
					"grabbing a buffer for flushing also detaches it.",
					len(ds.Rows), total, ns.rowCounts())
			}

			// Every row must be findable, and exactly once. The union across
			// nodes must reconstruct the input exactly.
			var all []minilog.Row
			for i := range ns.storages {
				rows, _ := ns.storages[i].Search(&minilog.Query{
					MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64,
				})
				all = append(all, rows...)
			}
			minilog.SortRows(all)
			want := make([]minilog.Row, len(ds.Rows))
			copy(want, ds.Rows)
			minilog.SortRows(want)
			if ok, msg := rowsEqual(all, want); !ok {
				t.Fatalf("union of all nodes != ingested rows: %s", msg)
			}

			t.Logf("MEASUREMENT %-6s: %d rows -> %v", policy, total, ns.rowCounts())
		})
	}
}

// TestStage7BalanceVsTheory checks sticky routing against balls-in-bins.
//
// This is the falsifiable prediction. Hashing S streams onto N nodes is
// exactly balls-in-bins, and the theory says the max/mean load ratio is
// about 1 + sqrt(2 ln N / (S/N)). Two things must hold:
//
//   - measured balance tracks the prediction as S/N grows
//   - measured balance is not BETTER than the prediction
//
// The second is the interesting one. Real hashing cannot beat the theory. If
// your numbers do, the router is not hashing: most likely it is round-robining,
// or the stream ids and the node count share a factor and the modulus is
// aliasing.
func TestStage7BalanceVsTheory(t *testing.T) {
	type row struct {
		streams, nodes int
		measured       float64
		predicted      float64
	}
	var results []row

	for _, nodes := range []int{2, 4, 8} {
		for _, streams := range []int{20, 100, 1000, 10000} {
			cfg := gen.DefaultConfig()
			cfg.Rows = 200_000
			cfg.Streams = streams
			ds := gen.Generate(cfg)

			ns := newNodeSet(t, nodes)
			ins := ns.insertClient(netinsert.Config{Routing: netinsert.RoutingSticky})
			ingestVia(t, ins, ns, ds, 10_000)

			results = append(results, row{
				streams:   streams,
				nodes:     nodes,
				measured:  balanceRatio(ns.rowCounts()),
				predicted: predictedBalanceRatio(streams, nodes),
			})
		}
	}

	t.Logf("MEASUREMENT ============= STICKY ROUTING vs BALLS-IN-BINS =============")
	t.Logf("MEASUREMENT %8s %6s %10s %10s %10s", "streams", "nodes", "S/N", "measured", "predicted")
	for _, r := range results {
		t.Logf("MEASUREMENT %8d %6d %10.1f %9.3fx %9.3fx",
			r.streams, r.nodes, float64(r.streams)/float64(r.nodes), r.measured, r.predicted)
	}
	t.Logf("MEASUREMENT ====================================================================")
	t.Logf("NOTE Balance improves as the SQUARE ROOT of streams-per-node. That is why\n" +
		"     'we have lots of streams so it will even out' is only half true: going\n" +
		"     from 100 to 10,000 streams buys you a factor of ten in S/N and only a\n" +
		"     factor of about three in imbalance.")

	for _, r := range results {
		excessMeasured := r.measured - 1
		excessPredicted := r.predicted - 1

		// Generous band: the formula is asymptotic and a single run is one
		// sample. What is being tested is order of magnitude and direction.
		if excessMeasured > 3*excessPredicted+0.05 {
			t.Errorf("streams=%d nodes=%d: measured imbalance %.3fx is far worse than the\n"+
				"predicted %.3fx. Something is concentrating streams: check that you are\n"+
				"MIXING streamID rather than using its low bits, and that the mix is not\n"+
				"cancelling against however StreamIDForLabels derives them.",
				r.streams, r.nodes, r.measured, r.predicted)
		}
		if excessPredicted > 0.05 && excessMeasured < excessPredicted/4 {
			t.Errorf("streams=%d nodes=%d: measured imbalance %.3fx is BETTER than the\n"+
				"theoretical %.3fx for random hashing. Hashing cannot beat balls-in-bins.\n"+
				"You are almost certainly round-robining rather than hashing -- which\n"+
				"balances beautifully here and falls apart the moment the input is\n"+
				"batched by stream, which real input always is.",
				r.streams, r.nodes, r.measured, r.predicted)
		}
	}
}

// TestStage7RoutingTradeoff is the deliverable of stage 7.
//
// Three policies against four stream-skew levels, reporting the two numbers
// that move in opposite directions:
//
//	balance       max node load / mean node load. Lower is better.
//	nodes touched how many nodes hold rows for one stream. Lower is better.
//
// Nothing wins both columns. That is the whole point, and it is why the real
// implementation is a hybrid rather than a choice.
func TestStage7RoutingTradeoff(t *testing.T) {
	const nodes = 4

	base := gen.DefaultConfig()
	base.Rows = 200_000
	base.Streams = 200
	baseDS := gen.Generate(base)

	type result struct {
		policy       netinsert.RoutingPolicy
		skew         float64
		balance      float64
		hotNodes     int
		coldNodes    int
		activeStream int
	}
	var results []result

	for _, skew := range []float64{0, 0.6, 1.0, 1.3} {
		ds := baseDS
		if skew > 0 {
			ds = skewStreams(baseDS, skew, 99)
		}

		// The hottest and coldest streams under this skew, measured from the
		// dataset itself rather than from anything under test.
		hot, cold := hottestColdestStream(ds)

		for _, policy := range []netinsert.RoutingPolicy{
			netinsert.RoutingSticky, netinsert.RoutingHybrid, netinsert.RoutingSpray,
		} {
			ns := newNodeSet(t, nodes)
			ins := ns.insertClient(netinsert.Config{Routing: policy})
			ingestVia(t, ins, ns, ds, 10_000)

			full := func(sid uint64) *minilog.Query {
				return &minilog.Query{
					MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64, StreamID: &sid,
				}
			}
			results = append(results, result{
				policy:    policy,
				skew:      skew,
				balance:   balanceRatio(ns.rowCounts()),
				hotNodes:  ns.nodesMatching(full(hot)),
				coldNodes: ns.nodesMatching(full(cold)),
			})
		}
	}

	t.Logf("MEASUREMENT ================= ROUTING TRADEOFF TABLE =================")
	t.Logf("MEASUREMENT (4 nodes, 200k rows, 200 streams)")
	t.Logf("MEASUREMENT %8s %8s %10s %14s %14s", "policy", "skew", "balance", "nodes/hot-str", "nodes/cold-str")
	for _, r := range results {
		t.Logf("MEASUREMENT %8s %8.1f %9.2fx %14d %14d",
			r.policy, r.skew, r.balance, r.hotNodes, r.coldNodes)
	}
	t.Logf("MEASUREMENT ====================================================================")
	t.Logf("NOTE Read the sticky rows down the skew column first. At skew 0 sticky\n" +
		"     looks fine. At skew 1.3 -- which is roughly what production log volume\n" +
		"     looks like -- one node is carrying the chattiest service alone, and\n" +
		"     adding nodes does not help, because the stream cannot be split.\n" +
		"\n" +
		"     Then read the hybrid row. It keeps cold streams on one node (cheap\n" +
		"     stream queries) while splitting hot ones (parallel scans of the data\n" +
		"     that actually matters). That is the entire argument for the 1000-row\n" +
		"     threshold, and you now have it in numbers you measured.\n" +
		"\n" +
		"     SAVE THIS TABLE.")

	// The invariants that must hold whatever the numbers say.
	for _, r := range results {
		if r.policy == netinsert.RoutingSticky && r.coldNodes != 1 {
			t.Errorf("sticky routing put a cold stream on %d nodes; sticky means one.", r.coldNodes)
		}
		if r.policy == netinsert.RoutingSpray && r.skew == 0 && r.balance > 1.05 {
			t.Errorf("spray routing has %.2fx imbalance at 200k rows; random per-row\n"+
				"assignment should be near-perfectly balanced at this scale.", r.balance)
		}
	}

	// The headline comparison: at high skew, hybrid must balance better than
	// sticky. If it does not, the threshold is never being crossed.
	var stickyHigh, hybridHigh float64
	for _, r := range results {
		if r.skew == 1.3 && r.policy == netinsert.RoutingSticky {
			stickyHigh = r.balance
		}
		if r.skew == 1.3 && r.policy == netinsert.RoutingHybrid {
			hybridHigh = r.balance
		}
	}
	if hybridHigh >= stickyHigh {
		t.Errorf("at skew 1.3, hybrid balance (%.2fx) is no better than sticky (%.2fx).\n"+
			"Hybrid should be spraying the hot streams. Either StickyRowsPerStream is\n"+
			"too large for this dataset (200k rows over 200 streams), or the per-stream\n"+
			"counter is not actually counting -- check that nodeForRow increments it\n"+
			"on EVERY call, not only on the first.", hybridHigh, stickyHigh)
	}
}

// hottestColdestStream returns the stream ids with the most and fewest rows.
// Measured from the dataset, independent of anything under test.
func hottestColdestStream(ds *gen.Dataset) (hot, cold uint64) {
	counts := make(map[uint64]int)
	for i := range ds.Rows {
		counts[ds.Rows[i].StreamID]++
	}
	first := true
	var hi, lo int
	for sid, c := range counts {
		if first || c > hi {
			hi, hot = c, sid
		}
		if first || c < lo {
			lo, cold = c, sid
		}
		first = false
	}
	return hot, cold
}

// TestStage7StickyThresholdSweep finds where the hybrid threshold matters.
//
// VictoriaLogs picked 1000. It is not derived from anything -- it is a
// judgement about where "small stream" stops. Find out whether your data
// agrees, and note that the right answer depends on rows-per-stream, which is
// a property of the workload and not of the code.
func TestStage7StickyThresholdSweep(t *testing.T) {
	const nodes = 4
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	cfg.Streams = 200
	ds := skewStreams(gen.Generate(cfg), 1.2, 7)
	hot, cold := hottestColdestStream(ds)

	t.Logf("MEASUREMENT ============ HYBRID THRESHOLD SWEEP (skew 1.2) ============")
	t.Logf("MEASUREMENT %10s %10s %14s %14s", "threshold", "balance", "nodes/hot-str", "nodes/cold-str")

	for _, threshold := range []int{10, 100, 1000, 10000, 1 << 30} {
		ns := newNodeSet(t, nodes)
		ins := ns.insertClient(netinsert.Config{
			Routing:             netinsert.RoutingHybrid,
			StickyRowsPerStream: threshold,
		})
		ingestVia(t, ins, ns, ds, 10_000)

		full := func(sid uint64) *minilog.Query {
			return &minilog.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64, StreamID: &sid}
		}
		label := itoa(threshold)
		if threshold == 1<<30 {
			label = "never" // pure sticky
		}
		t.Logf("MEASUREMENT %10s %9.2fx %14d %14d",
			label, balanceRatio(ns.rowCounts()), ns.nodesMatching(full(hot)), ns.nodesMatching(full(cold)))
	}
	t.Logf("MEASUREMENT ====================================================================")
	t.Logf("NOTE threshold=never is pure sticky; threshold=10 is nearly pure spray.\n" +
		"     The useful threshold is the one that leaves cold streams on 1 node\n" +
		"     while spreading the hot one. Where that sits depends entirely on\n" +
		"     rows-per-stream, which is a fact about the workload -- which is why\n" +
		"     this is a constant in the source and not a computed value.")
}

// TestStage7BatchSizeKnee measures the batching tradeoff.
//
// Bigger batches amortise HTTP and compress better; smaller batches make rows
// visible sooner. Somewhere between the two there is a knee, and past it you
// are paying latency for throughput you are not getting.
func TestStage7BatchSizeKnee(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput measurement; skipped under -short")
	}
	cfg := gen.DefaultConfig()
	cfg.Rows = 300_000
	ds := gen.Generate(cfg)

	t.Logf("MEASUREMENT ================== BATCH SIZE SWEEP ==================")
	t.Logf("MEASUREMENT %12s %12s %14s %12s", "blockSize", "ingest(s)", "rows/s", "requests")

	for _, size := range []int{64 << 10, 256 << 10, 1 << 20, 2 << 20, 8 << 20} {
		ns := newNodeSet(t, 4)
		ins := ns.insertClient(netinsert.Config{
			MaxInsertBlockSize: size,
			// Long interval so the sweep measures block size, not the ticker.
			FlushInterval: time.Hour,
		})

		start := time.Now()
		ingestVia(t, ins, ns, ds, 5_000)
		elapsed := time.Since(start)

		var requests int64
		for _, srv := range ns.servers {
			requests += srv.requests.Load()
		}

		t.Logf("MEASUREMENT %11dK %12.3f %14.0f %12d",
			size>>10, elapsed.Seconds(), float64(len(ds.Rows))/elapsed.Seconds(), requests)
	}
	t.Logf("MEASUREMENT ====================================================================")
	t.Logf("NOTE Request count is the number that explains the throughput column.\n" +
		"     Then remember what this sweep is NOT showing: with FlushInterval at\n" +
		"     its real value, block size also sets how long a row sits in this\n" +
		"     process before anyone can query it -- and a row buffered in a client\n" +
		"     is a row that process death loses. That is the cost of the weakened\n" +
		"     MustAddRows contract, and it is bounded by FlushInterval, not by\n" +
		"     anything a storage node does.")
}

// TestStage7RouterIsSafeUnderConcurrency ingests from many goroutines at once.
//
// The hybrid router's per-stream counter is shared mutable state on the hot
// path of every row. Run this under -race.
func TestStage7RouterIsSafeUnderConcurrency(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	ns := newNodeSet(t, 4)
	ins := ns.insertClient(netinsert.Config{Routing: netinsert.RoutingHybrid})

	const writers = 8
	chunk := len(ds.Rows) / writers
	done := make(chan struct{}, writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			lo := w * chunk
			hi := lo + chunk
			if w == writers-1 {
				hi = len(ds.Rows)
			}
			for off := lo; off < hi; off += 1000 {
				end := off + 1000
				if end > hi {
					end = hi
				}
				// MustAddRows sorts in place, so hand it a copy -- concurrent
				// writers must not share a backing array.
				batch := make([]minilog.Row, end-off)
				copy(batch, ds.Rows[off:end])
				ins.MustAddRows(batch)
			}
		}(w)
	}
	for w := 0; w < writers; w++ {
		<-done
	}
	ins.MustForceFlush()
	ns.flushAll()

	total := 0
	for _, c := range ns.rowCounts() {
		total += c
	}
	if total != len(ds.Rows) {
		t.Fatalf("concurrent ingest: %d rows in, %d landed (%v).\n"+
			"A lost row under concurrency and not under a single writer means the\n"+
			"pending buffer swap is not atomic: two goroutines grabbed the same\n"+
			"buffer, or one wrote into a buffer another had already detached.",
			len(ds.Rows), total, ns.rowCounts())
	}
	t.Logf("MEASUREMENT %d writers, %d rows, all accounted for: %v",
		writers, total, ns.rowCounts())
}
