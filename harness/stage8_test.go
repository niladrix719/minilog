package harness

// ---------------------------------------------------------------------------
// STAGE 8 MEASUREMENTS -- COMPLETE.
//
//	go test ./harness -run TestStage8 -v
//	go test ./harness -run TestStage8 -race    <- run this one too
//
// Fan-out and merge. The correctness measurement here is the cheapest one in
// the whole exercise, because the oracle already existed: bruteForce scans a
// []Row and does not care how many machines produced it.
// ---------------------------------------------------------------------------

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
	"github.com/niladrix719/minilog/netinsert"
	"github.com/niladrix719/minilog/netstorage"
)

// newClusterWith builds an N-node cluster and ingests ds through it.
func newClusterWith(t *testing.T, nodes int, policy netinsert.RoutingPolicy, ds *gen.Dataset) (*nodeSet, *netstorage.Storage) {
	t.Helper()
	ns := newNodeSet(t, nodes)
	c := ns.cluster(netstorage.Config{
		Insert: netinsert.Config{Routing: policy},
	})
	ingest(t, c, ds, 10_000)
	ns.flushAll()
	return ns, c
}

// TestStage8ClusterMatchesBruteForce is the headline stage 8 measurement.
//
// 1000 random queries, four cluster sizes, three routing policies. Every
// result must equal a brute-force scan exactly: same rows, same order.
//
// The routing policy sweep is not decoration. Under sticky routing a stream
// lives entirely on one node, so the merge never has to interleave two nodes'
// rows for the same stream -- and a merge that mishandles exactly that case
// passes every sticky test and fails in production the day a stream gets busy.
// Spray guarantees the interleave on every stream. If only one of these three
// fails, the failure tells you where to look.
func TestStage8ClusterMatchesBruteForce(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	cfg.Streams = 100
	ds := gen.Generate(cfg)

	queries := randomQueries(ds, 1000, 20250826)

	for _, nodes := range []int{1, 2, 4, 8} {
		for _, policy := range []netinsert.RoutingPolicy{
			netinsert.RoutingSticky, netinsert.RoutingHybrid, netinsert.RoutingSpray,
		} {
			name := sprintf("nodes=%d/%s", nodes, policy)
			t.Run(name, func(t *testing.T) {
				_, c := newClusterWith(t, nodes, policy, ds)

				for i, q := range queries {
					got, ss := c.Search(q)
					want := bruteForce(ds.Rows, q)
					if ok, msg := rowsEqual(got, want); !ok {
						t.Fatalf("%s: query %d disagrees with brute force: %s\n"+
							"  query: %+v\n"+
							"  stats: %s\n"+
							"Where to look, in order:\n"+
							"  - wrong ORDER only: the merge compares streamID but not\n"+
							"    timestamp, or the nodes did not return sorted rows.\n"+
							"  - MISSING rows, spray only: the merge drops one side of a\n"+
							"    (streamID, timestamp) tie. Ties are legal -- nothing in this\n"+
							"    system makes a row unique.\n"+
							"  - missing rows, ALL policies: a node's error was swallowed and\n"+
							"    counted as an empty result.\n"+
							"  - EXTRA rows: a node was queried twice, or the address list\n"+
							"    contains a duplicate.",
							name, i, msg, q, ss)
					}
					if ss.IsPartial() {
						t.Fatalf("%s: query %d reported PARTIAL with every node healthy: %s",
							name, i, ss)
					}
					if ss.NodesQueried != nodes {
						t.Fatalf("%s: query %d reports NodesQueried=%d, want %d.\n"+
							"Every query goes to every node -- that is the design. Set this\n"+
							"once in netselect.Storage.Search after the fan-out.",
							name, i, ss.NodesQueried, nodes)
					}
				}
			})
		}
	}
	t.Logf("MEASUREMENT %d random queries x {1,2,4,8} nodes x 3 policies == brute force",
		len(queries))
}

// randomQueries builds a varied query set: time ranges, stream filters, token
// filters, conjunctions, and combinations of all of them.
func randomQueries(ds *gen.Dataset, n int, seed int64) []*minilog.Query {
	rng := rand.New(rand.NewSource(seed))
	lo, hi := ds.Rows[0].Timestamp, ds.Rows[len(ds.Rows)-1].Timestamp
	span := hi - lo
	absent := gen.AbsentTokens(16, seed)
	tokens := []string{gen.RareToken, gen.CommonToken, "error", "info", "GET", "POST", "200", "500"}
	columns := []string{"_msg", "level", "method", "status", "service"}

	out := make([]*minilog.Query, n)
	for i := range out {
		q := &minilog.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}

		if rng.Intn(2) == 0 {
			a := lo + rng.Int63n(span+1)
			b := lo + rng.Int63n(span+1)
			if a > b {
				a, b = b, a
			}
			q.MinTimestamp, q.MaxTimestamp = a, b
		}
		if rng.Intn(4) == 0 {
			sid := ds.Streams[rng.Intn(len(ds.Streams))].ID
			q.StreamID = &sid
		}
		for f := rng.Intn(3); f > 0; f-- {
			tok := tokens[rng.Intn(len(tokens))]
			if rng.Intn(8) == 0 {
				tok = absent[rng.Intn(len(absent))]
			}
			q.Filters = append(q.Filters, minilog.Filter{
				Column: columns[rng.Intn(len(columns))],
				Token:  tok,
			})
		}
		out[i] = q
	}
	return out
}

// TestStage8MergePreservesTies checks the merge keeps duplicate keys.
//
// Two rows with the same (StreamID, Timestamp) and different fields are legal
// and end up on different nodes under spray routing. A heap merge that
// advances both readers when the keys compare equal silently drops one, and
// the result still looks sorted, still looks plausible, and is short by
// exactly the number of collisions.
//
// This is the same bug the stage 3 merge harness exists to catch, one level
// up. It is worth noticing that the bug survived the move to a new layer.
func TestStage8MergePreservesTies(t *testing.T) {
	base := gen.DefaultConfig()
	base.Rows = 20_000
	base.Streams = 4
	ds := gen.Generate(base)

	// Collapse every timestamp onto a small set so ties are guaranteed and
	// frequent, then re-sort into the canonical order.
	for i := range ds.Rows {
		ds.Rows[i].Timestamp = ds.Rows[i].Timestamp / 1e9 * 1e9
	}
	minilog.SortRows(ds.Rows)

	ties := 0
	for i := 1; i < len(ds.Rows); i++ {
		if ds.Rows[i].StreamID == ds.Rows[i-1].StreamID && ds.Rows[i].Timestamp == ds.Rows[i-1].Timestamp {
			ties++
		}
	}
	if ties == 0 {
		t.Fatal("test setup produced no tied keys")
	}

	// Spray routing guarantees tied rows are split across nodes.
	_, c := newClusterWith(t, 4, netinsert.RoutingSpray, ds)

	q := &minilog.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}
	got, _ := c.Search(q)
	want := bruteForce(ds.Rows, q)

	if len(got) != len(want) {
		t.Fatalf("merge returned %d rows, want %d (%d tied keys in the dataset).\n"+
			"A k-way merge must emit EVERY row whose key compares equal, not one\n"+
			"per distinct key. Advance exactly one reader per emitted row.",
			len(got), len(want), ties)
	}
	if ok, msg := rowsEqual(got, want); !ok {
		t.Fatalf("merge with %d tied keys: %s", ties, msg)
	}
	t.Logf("MEASUREMENT %d rows with %d tied (streamID, timestamp) keys survived a 4-node merge",
		len(got), ties)
}

// TestStage8ScalingCurve measures latency against node count, and checks it
// against an Amdahl prediction derived from measured merge throughput.
//
// The prediction: total time is per-node scan time (which falls as 1/N) plus
// merge time (which does not fall at all, and grows slowly with N). Speedup
// therefore flattens, and it flattens at a point you can compute BEFORE you
// look at the measurement.
func TestStage8ScalingCurve(t *testing.T) {
	if testing.Short() {
		t.Skip("latency measurement; skipped under -short")
	}
	cfg := gen.DefaultConfig()
	cfg.Rows = 400_000
	cfg.Streams = 200
	ds := gen.Generate(cfg)

	// A broad query: most of the work is scanning, which is the part that
	// parallelises. A selective query would be dominated by fixed costs and
	// would tell you nothing about scaling.
	q := &minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		Filters:      []minilog.Filter{{Column: "level", Token: "info"}},
	}

	type point struct {
		nodes   int
		latency time.Duration
		rows    int
	}
	var points []point

	for _, nodes := range []int{1, 2, 4, 8} {
		_, c := newClusterWith(t, nodes, netinsert.RoutingSpray, ds)

		// Warm up, then take the best of five -- the minimum is the most
		// stable statistic on a shared machine, since noise only ever adds.
		c.Search(q)
		best := time.Duration(math.MaxInt64)
		var rows int
		for i := 0; i < 5; i++ {
			start := time.Now()
			got, _ := c.Search(q)
			if d := time.Since(start); d < best {
				best = d
			}
			rows = len(got)
		}
		points = append(points, point{nodes: nodes, latency: best, rows: rows})
	}

	t.Logf("MEASUREMENT ================== SCALING CURVE ==================")
	t.Logf("MEASUREMENT query: level:info over %d rows -> %d matches", len(ds.Rows), points[0].rows)
	t.Logf("MEASUREMENT %8s %14s %12s %12s", "nodes", "latency(ms)", "speedup", "ideal")
	base := points[0].latency
	for _, p := range points {
		t.Logf("MEASUREMENT %8d %14.2f %11.2fx %11.2fx",
			p.nodes, float64(p.latency.Microseconds())/1000,
			float64(base)/float64(p.latency), float64(p.nodes))
	}
	t.Logf("MEASUREMENT ====================================================================")
	t.Logf("NOTE Speedup falls short of ideal and the gap widens with N. Two reasons,\n" +
		"     and you can separate them:\n" +
		"       - the MERGE is serial. It processes every matched row exactly once\n" +
		"         no matter how many nodes produced them, so its cost is flat in N\n" +
		"         while the scan's cost falls as 1/N. Predict the crossover from\n" +
		"         your measured merge throughput and check it against this table.\n" +
		"       - fan-out is not free. Every query pays N requests, N responses and\n" +
		"         N deserialisations even when N-1 nodes have nothing to say.\n" +
		"     These tests run every node in one process on loopback, so network\n" +
		"     latency is near zero here and real deployments look WORSE. Treat this\n" +
		"     curve as an upper bound.")

	if len(points) >= 2 && points[len(points)-1].latency >= points[0].latency {
		t.Errorf("8 nodes (%v) is not faster than 1 node (%v) on a broad query.\n"+
			"The fan-out is not running concurrently -- check that the per-node\n"+
			"requests are issued from separate goroutines and awaited together,\n"+
			"rather than in a loop.", points[len(points)-1].latency, points[0].latency)
	}
	for _, p := range points {
		if p.rows != points[0].rows {
			t.Fatalf("node count changed the answer: %d nodes returned %d rows, 1 node returned %d",
				p.nodes, p.rows, points[0].rows)
		}
	}
}

// TestStage8FanoutTax measures what fan-out-to-all costs on a selective query.
//
// This is the bill for the design decision that makes everything else simple.
// Because a query hits every node, a stream-filtered query under sticky
// routing opens N nodes to get an answer that lives entirely on one.
//
// The nodes-that-actually-had-rows figure is measured by querying each node's
// local storage directly, so it shares no code with the fan-out path.
func TestStage8FanoutTax(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	cfg.Streams = 200
	ds := gen.Generate(cfg)

	t.Logf("MEASUREMENT ==================== FAN-OUT TAX ====================")
	t.Logf("MEASUREMENT %8s %8s %12s %14s %14s", "policy", "nodes", "queried", "had rows", "wasted")

	for _, policy := range []netinsert.RoutingPolicy{
		netinsert.RoutingSticky, netinsert.RoutingHybrid, netinsert.RoutingSpray,
	} {
		for _, nodes := range []int{2, 4, 8} {
			ns, c := newClusterWith(t, nodes, policy, ds)

			sid := ds.Streams[5].ID
			q := &minilog.Query{
				MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64, StreamID: &sid,
			}
			_, ss := c.Search(q)
			useful := ns.nodesMatching(q)

			t.Logf("MEASUREMENT %8s %8d %12d %14d %14d",
				policy, nodes, ss.NodesQueried, useful, ss.NodesQueried-useful)
		}
	}
	t.Logf("MEASUREMENT ====================================================================")
	t.Logf("NOTE The 'wasted' column is the price of never needing consistent hashing,\n" +
		"     a membership protocol, or a rebalancing story. A routing table that\n" +
		"     told the select node WHERE a stream lives would drive that column to\n" +
		"     zero -- and would have to be kept correct across node additions,\n" +
		"     removals and re-routed writes, by every client, forever.\n" +
		"     VictoriaLogs pays this column instead. Decide whether you agree, and\n" +
		"     write down why.")
}

// TestStage8StatsAreMerged checks the per-node stats are folded, not dropped.
//
// SearchStats.Merge already existed and was already folding per-shard stats in
// Storage.Search. This is the same call one level up, and if the numbers come
// back as zeros then the cluster has no instrumentation at all -- which stages
// 9 and 10 need.
func TestStage8StatsAreMerged(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	ds := gen.Generate(cfg)

	_, c1 := newClusterWith(t, 1, netinsert.RoutingSpray, ds)
	_, c8 := newClusterWith(t, 8, netinsert.RoutingSpray, ds)

	q := &minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		Filters:      []minilog.Filter{{Column: "_msg", Token: gen.RareToken}},
	}
	_, ss1 := c1.Search(q)
	_, ss8 := c8.Search(q)

	t.Logf("MEASUREMENT 1 node:  %s", ss1)
	t.Logf("MEASUREMENT 8 nodes: %s", ss8)

	if ss8.BlocksTotal == 0 {
		t.Fatalf("BlocksTotal is 0 across 8 nodes -- per-node SearchStats are being\n" +
			"discarded rather than merged. Use SearchStats.Merge, which already\n" +
			"exists and is already used for cross-shard folding in Storage.Search.")
	}
	if ss8.BytesReceivedFromNodes == 0 {
		t.Errorf("BytesReceivedFromNodes is 0. Count the response body length in\n" +
			"storageNode.runQuery -- stage 10's entire measurement is this counter.")
	}
	if ss8.NodesQueried != 8 {
		t.Errorf("NodesQueried=%d, want 8", ss8.NodesQueried)
	}
	if ss8.BlocksTotal <= ss1.BlocksTotal/2 {
		t.Errorf("8 nodes considered %d blocks, 1 node considered %d.\n"+
			"Spreading the same rows over more nodes produces more, smaller blocks --\n"+
			"it should not produce dramatically fewer. Stats are being lost.",
			ss8.BlocksTotal, ss1.BlocksTotal)
	}
}
