package harness

// ---------------------------------------------------------------------------
// STAGE 10 MEASUREMENTS -- COMPLETE.
//
//	go test ./harness -run TestStage10 -v
//
// Stages 6-9 shipped every matching row to the select node. This stage is
// where you feel why that does not scale, and then fix it with the smallest
// honest thing: push Limit down to the storage nodes.
//
// The correctness measurement matters more than the performance one. A limit
// pushdown that returns the wrong rows produces a plausible answer, and by
// stage 9 you should distrust plausible answers on principle.
// ---------------------------------------------------------------------------

import (
	"math"
	"runtime"
	"testing"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
	"github.com/niladrix719/minilog/netinsert"
)

// TestStage10LimitMatchesBruteForce is the correctness gate.
//
// A limited query must return exactly the first Limit rows of the unlimited
// answer, in the same order. Not "Limit rows that match" -- the FIRST Limit,
// which is what makes the pushdown sound.
//
// This also exercises the local engine: Storage.Search must honour q.Limit
// too, or the 1-node case disagrees with everything else.
func TestStage10LimitMatchesBruteForce(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	cfg.Streams = 100
	ds := gen.Generate(cfg)

	limits := []int{1, 10, 100, 1000, 50_000}

	for _, nodes := range []int{1, 4, 8} {
		for _, policy := range []netinsert.RoutingPolicy{
			netinsert.RoutingSticky, netinsert.RoutingSpray,
		} {
			name := sprintf("nodes=%d/%s", nodes, policy)
			t.Run(name, func(t *testing.T) {
				_, c := newClusterWith(t, nodes, policy, ds)

				for _, limit := range limits {
					for qi, base := range randomQueries(ds, 40, int64(limit)) {
						q := *base
						q.Limit = limit

						got, ss := c.Search(&q)

						unlimited := *base
						unlimited.Limit = 0
						want := (&q).ApplyLimit(bruteForce(ds.Rows, &unlimited))

						if ok, msg := rowsEqual(got, want); !ok {
							t.Fatalf("%s limit=%d query %d: %s\n"+
								"  query: %+v\n"+
								"  stats: %s\n"+
								"The pushdown is sound only if each node returns its own FIRST\n"+
								"Limit rows in (StreamID, Timestamp) order, and the select node\n"+
								"merges then truncates. Common breakages:\n"+
								"  - node applies Limit BEFORE filtering -> wrong rows\n"+
								"  - node returns an arbitrary Limit rows -> right count, wrong set\n"+
								"  - select node truncates BEFORE merging -> drops rows that sort\n"+
								"    early on a node that happened to be read late",
								name, limit, qi, msg, &q, ss)
						}
						if limit > 0 && len(got) > limit {
							t.Fatalf("%s limit=%d returned %d rows", name, limit, len(got))
						}
					}
				}
			})
		}
	}
	t.Logf("MEASUREMENT limits %v x {1,4,8} nodes x 2 policies == first-N of brute force", limits)
}

// TestStage10PushdownCutsWireBytes is the headline performance measurement.
//
// Without pushdown, every matching row crosses the network and is then thrown
// away by the select node. With it, at most Limit rows per node cross.
//
// The counter is SearchStats.BytesReceivedFromNodes, measured at the socket.
func TestStage10PushdownCutsWireBytes(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 400_000
	cfg.Streams = 200
	ds := gen.Generate(cfg)

	const nodes = 8
	_, c := newClusterWith(t, nodes, netinsert.RoutingSpray, ds)

	// A broad query: most rows match, so the limit does almost all the work.
	broad := &minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		Filters:      []minilog.Filter{{Column: "level", Token: "info"}},
	}

	unlimited := *broad
	gotAll, ssAll := c.Search(&unlimited)

	t.Logf("MEASUREMENT ================== LIMIT PUSHDOWN ==================")
	t.Logf("MEASUREMENT %10s %12s %16s %14s", "limit", "rows", "wire(MiB)", "vs unlimited")
	t.Logf("MEASUREMENT %10s %12d %16.2f %14s",
		"none", len(gotAll), float64(ssAll.BytesReceivedFromNodes)/(1<<20), "1.00x")

	for _, limit := range []int{100, 1000, 10000} {
		q := *broad
		q.Limit = limit
		got, ss := c.Search(&q)

		ratio := 0.0
		if ss.BytesReceivedFromNodes > 0 {
			ratio = float64(ssAll.BytesReceivedFromNodes) / float64(ss.BytesReceivedFromNodes)
		}
		t.Logf("MEASUREMENT %10d %12d %16.4f %13.1fx",
			limit, len(got), float64(ss.BytesReceivedFromNodes)/(1<<20), ratio)

		if ss.BytesReceivedFromNodes >= ssAll.BytesReceivedFromNodes {
			t.Errorf("limit=%d moved %d bytes over the network; the unlimited query moved\n"+
				"%d. The limit is not reaching the storage nodes -- it is being applied\n"+
				"only after the rows have already crossed the wire, which is exactly the\n"+
				"cost this stage exists to remove. Check that MarshalQuery ships Limit\n"+
				"and that handleSelect applies it.",
				limit, ss.BytesReceivedFromNodes, ssAll.BytesReceivedFromNodes)
		}

		// Each node may legitimately return up to Limit rows, so the select
		// node receives at most nodes*Limit. Much more than that means the
		// nodes are not truncating at all.
		if limit*nodes*2 < len(gotAll) {
			maxExpected := float64(limit*nodes) / float64(len(gotAll))
			if float64(ss.BytesReceivedFromNodes) > float64(ssAll.BytesReceivedFromNodes)*maxExpected*3 {
				t.Errorf("limit=%d over %d nodes should move roughly %d rows' worth of bytes,\n"+
					"but moved %.1f%% of the unlimited traffic.",
					limit, nodes, limit*nodes,
					100*float64(ss.BytesReceivedFromNodes)/float64(ssAll.BytesReceivedFromNodes))
			}
		}
	}
	t.Logf("MEASUREMENT ====================================================================")
	t.Logf("NOTE This is splitQueryToRemoteAndLocal (lib/logstorage/net_query_runner.go)\n" +
		"     reduced to its smallest honest form. Read theirs now. The shape will be\n" +
		"     familiar; what will not be obvious is WHICH pipes can be pushed down.\n" +
		"     A sort can. A limit can, as you just proved. A uniq cannot -- work out\n" +
		"     why before you look it up, because the reason generalises to every\n" +
		"     distributed query planner you will ever read.")
}

// TestStage10SelectNodeMemoryScaling shows why the pushdown exists.
//
// Without it, the select node materialises every node's full result at once,
// so peak allocation grows linearly in node count for the same dataset. With
// it, the line is flat.
//
// Measured with runtime.MemStats around the query. It is a coarse instrument
// -- GC timing makes single readings noisy -- so the interesting output is the
// SHAPE of the column, not any one number.
func TestStage10SelectNodeMemoryScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("allocation measurement; skipped under -short")
	}
	cfg := gen.DefaultConfig()
	cfg.Rows = 300_000
	cfg.Streams = 200
	ds := gen.Generate(cfg)

	broad := &minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		Filters:      []minilog.Filter{{Column: "level", Token: "info"}},
	}

	t.Logf("MEASUREMENT ============ SELECT-NODE ALLOCATION vs NODES ============")
	t.Logf("MEASUREMENT %8s %16s %16s", "nodes", "no limit(MiB)", "limit=100(MiB)")

	for _, nodes := range []int{1, 2, 4, 8} {
		_, c := newClusterWith(t, nodes, netinsert.RoutingSpray, ds)

		noLimit := allocatedDuring(func() {
			q := *broad
			c.Search(&q)
		})
		withLimit := allocatedDuring(func() {
			q := *broad
			q.Limit = 100
			c.Search(&q)
		})

		t.Logf("MEASUREMENT %8d %16.1f %16.1f",
			nodes, float64(noLimit)/(1<<20), float64(withLimit)/(1<<20))
	}
	t.Logf("MEASUREMENT ====================================================================")
	t.Logf("NOTE The left column slopes and the right one should not. Where the left\n" +
		"     column crosses your select node's memory limit is the cluster size at\n" +
		"     which the un-pushed version takes the process down -- and it does so on\n" +
		"     the day somebody adds a node, not on the day somebody writes the query.\n" +
		"\n" +
		"     Pushdown flattens the line but does not remove the underlying problem:\n" +
		"     MarshalQueryResponse still buffers a whole node result before sending,\n" +
		"     and Search still returns a materialised []Row. VictoriaLogs streams\n" +
		"     length-prefixed blocks in both directions so that neither side ever\n" +
		"     holds a full result set. That is the next thing you would build, and\n" +
		"     you now have the number that justifies it.")
}

// allocatedDuring returns bytes allocated by f, best-effort.
func allocatedDuring(f func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	if after.TotalAlloc < before.TotalAlloc {
		return 0
	}
	return after.TotalAlloc - before.TotalAlloc
}

// TestStage10LimitDoesNotBreakPartialResponses checks the two features
// compose.
//
// Limit and AllowPartialResponse interact in a way that is easy to get wrong:
// with a node down, a limited query can still return a FULL Limit rows, and
// they will still all be real rows -- but they are not the right rows, because
// the missing node held some that sort earlier.
//
// So a limited partial result is a subset of nothing in particular. It must
// still be marked partial, and that marking is the only thing standing between
// the caller and a completely wrong answer that looks completely normal.
func TestStage10LimitDoesNotBreakPartialResponses(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	ns, c := newClusterWith(t, 4, netinsert.RoutingSpray, ds)

	q := &minilog.Query{
		MinTimestamp:         math.MinInt64,
		MaxTimestamp:         math.MaxInt64,
		Limit:                500,
		AllowPartialResponse: true,
	}

	healthy, ssHealthy := c.Search(q)
	if ssHealthy.IsPartial() {
		t.Fatalf("healthy cluster reported partial: %s", ssHealthy)
	}
	if len(healthy) != 500 {
		t.Fatalf("limit=500 on a healthy cluster returned %d rows", len(healthy))
	}

	ns.kill(1)
	degraded, ssDegraded := c.Search(q)
	ns.revive(1)

	t.Logf("MEASUREMENT healthy:  %d rows, %s", len(healthy), ssHealthy)
	t.Logf("MEASUREMENT 1 down:   %d rows, %s", len(degraded), ssDegraded)

	if !ssDegraded.IsPartial() {
		t.Fatalf("a node is down and a limited query reported COMPLETE: %s\n"+
			"This is the most dangerous shape the bug takes. The row COUNT is still\n"+
			"%d, so nothing downstream looks suspicious -- but the rows are wrong,\n"+
			"because the missing node held rows that sort earlier than some of these.",
			ssDegraded, len(degraded))
	}
	if len(degraded) > 500 {
		t.Errorf("limit=500 returned %d rows", len(degraded))
	}
	t.Logf("NOTE Note the shape of the danger here: with a node down, the limited\n" +
		"     query still returned a plausible row count. Only the partial flag\n" +
		"     distinguishes it from a correct answer.")
}
