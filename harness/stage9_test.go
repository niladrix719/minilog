package harness

// ---------------------------------------------------------------------------
// STAGE 9 MEASUREMENTS -- COMPLETE.
//
//	go test ./harness -run TestStage9 -v
//	go run ./cmd/chaostest -iters 200 -nodes 4
//
// Nodes fail. Two paths, two different right answers:
//
//	insert  stay up. Re-route around the dead node; lose nothing.
//	select  fail loudly, or degrade VISIBLY. Never silently.
//
// The single most important assertion in this file is
// TestStage9PartialIsNeverSilent. A partial result presented as complete is
// the worst failure a distributed query system can have, because it is
// indistinguishable from a correct answer at every layer above it -- the
// dashboard renders, the alert does not fire, and nobody finds out.
// ---------------------------------------------------------------------------

import (
	"math"
	"testing"
	"time"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
	"github.com/niladrix719/minilog/netinsert"
	"github.com/niladrix719/minilog/netstorage"
)

// ---------------------------------------------------------------------------
// insert path
// ---------------------------------------------------------------------------

// TestStage9IngestSurvivesNodeLoss is the insert-path invariant.
//
// With at least one node reachable, every row handed to MustAddRows before
// MustForceFlush returned must be findable afterwards. Which node it landed on
// is not part of the contract -- and it cannot be, because a re-routed block
// lands somewhere the router would never have chosen and stays there forever.
//
// That is fine, and the reason it is fine is the same reason this whole design
// works: queries hit every node, so "the wrong node" is not a category that
// exists at read time.
func TestStage9IngestSurvivesNodeLoss(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 120_000
	ds := gen.Generate(cfg)

	ns := newNodeSet(t, 4)
	ins := ns.insertClient(netinsert.Config{
		Routing:             netinsert.RoutingHybrid,
		NodeDisableDuration: 200 * time.Millisecond,
	})

	// Ingest the first third with everything healthy.
	third := len(ds.Rows) / 3
	for off := 0; off < third; off += 5000 {
		ins.MustAddRows(ds.Rows[off:minInt(off+5000, third)])
	}
	ins.MustForceFlush()

	// Kill a node and keep ingesting through it.
	ns.kill(1)
	for off := third; off < 2*third; off += 5000 {
		ins.MustAddRows(ds.Rows[off:minInt(off+5000, 2*third)])
	}
	ins.MustForceFlush()

	// Bring it back and finish.
	ns.revive(1)
	for off := 2 * third; off < len(ds.Rows); off += 5000 {
		ins.MustAddRows(ds.Rows[off:minInt(off+5000, len(ds.Rows))])
	}
	ins.MustForceFlush()
	ns.flushAll()

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
		t.Fatalf("rows were lost or duplicated across a node outage: %s\n\n"+
			"counts per node: %v\n"+
			"  FEWER rows: a send failed and the buffer was dropped instead of\n"+
			"    re-routed. A failed send must retry elsewhere, and keep retrying\n"+
			"    while any node is up -- returning from MustForceFlush with data\n"+
			"    still buffered is the same as losing it.\n"+
			"  MORE rows: a buffer was re-routed AND the original send also landed.\n"+
			"    A send that times out may still have been applied; that is why\n"+
			"    re-routing is an at-least-once story. If you want exactly-once\n"+
			"    here you need idempotency keys, which is a much larger design.",
			msg, ns.rowCounts())
	}

	t.Logf("MEASUREMENT %d rows survived a node outage mid-ingest: %v", len(all), ns.rowCounts())
	t.Logf("MEASUREMENT rerouted rows by node: %v", ins.NodeReroutedRows())
}

// TestStage9RerouteBalance checks re-routing spreads across the survivors.
//
// The bug this exists to find: "send to any available node" implemented as
// "send to the first available node". It works, every test of correctness
// passes, and in production the node that happens to be first in the address
// list absorbs the entire load of every failure.
func TestStage9RerouteBalance(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	ds := gen.Generate(cfg)

	ns := newNodeSet(t, 4)
	ins := ns.insertClient(netinsert.Config{
		Routing:             netinsert.RoutingHybrid,
		NodeDisableDuration: 50 * time.Millisecond,
		MaxInsertBlockSize:  256 << 10, // small blocks: many re-route decisions
	})

	ns.kill(0)
	ingestVia(t, ins, ns, ds, 2_000)
	ns.revive(0)

	counts := ns.rowCounts()
	survivors := counts[1:]

	t.Logf("MEASUREMENT rows per node with node 0 down: %v", counts)
	t.Logf("MEASUREMENT reroutes by destination:        %v", ins.NodeReroutedRows())

	if counts[0] != 0 {
		t.Errorf("node 0 was down for the whole ingest but holds %d rows", counts[0])
	}

	ratio := balanceRatio(survivors)
	t.Logf("MEASUREMENT survivor balance: %.2fx", ratio)
	if ratio > 1.6 {
		t.Errorf("the three surviving nodes are unbalanced at %.2fx (%v).\n"+
			"Re-routing is concentrating on one survivor. Pick a RANDOM starting\n"+
			"index when scanning for an available node -- starting at 0 means every\n"+
			"client in the fleet re-routes to the same machine, and the node that\n"+
			"replaces a dead one inherits the load of the entire cluster.",
			ratio, survivors)
	}
}

// TestStage9DisabledNodeIsNotRetriedEveryBatch checks the cool-off works.
//
// Without it, every batch pays a connection failure against the dead node
// before re-routing, and ingest throughput collapses to the speed of the
// failure path. The cool-off is what turns "one slow batch" into "one slow
// batch every NodeDisableDuration".
func TestStage9DisabledNodeIsNotRetriedEveryBatch(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	ns := newNodeSet(t, 4)
	ins := ns.insertClient(netinsert.Config{
		NodeDisableDuration: 10 * time.Second, // long: no re-enable during the test
		MaxInsertBlockSize:  128 << 10,        // many blocks
	})

	ns.kill(2)
	before := ns.servers[2].requests.Load()
	ingestVia(t, ins, ns, ds, 2_000)
	attempts := ns.servers[2].requests.Load() - before
	ns.revive(2)

	// The gated listener drops connections before the handler runs, so a
	// blocked node records no requests. Count what the survivors saw instead:
	// if the client is not backing off, total traffic balloons.
	var survivorReqs int64
	for i, srv := range ns.servers {
		if i != 2 {
			survivorReqs += srv.requests.Load()
		}
	}
	t.Logf("MEASUREMENT dead node handler invocations: %d (expected 0 -- it never accepted)", attempts)
	t.Logf("MEASUREMENT survivor requests:             %d", survivorReqs)
	t.Logf("NOTE There is no assertion on retry COUNT here, because a dropped\n" +
		"     connection leaves no server-side trace. Verify the cool-off yourself:\n" +
		"     time this test with NodeDisableDuration at 10s and at 0. If the two\n" +
		"     are the same, you are not backing off, and the difference is a full\n" +
		"     TCP failure per batch -- which on a real network is a timeout, not an\n" +
		"     instant refusal, and is the difference between a degraded cluster and\n" +
		"     a stopped one.")
}

// ---------------------------------------------------------------------------
// select path
// ---------------------------------------------------------------------------

// TestStage9StrictQueryFailsOnNodeLoss checks the default behaviour.
//
// Default is complete-or-nothing. VictoriaLogs returns 502 here, deliberately,
// and the reasoning in their docs is worth reading: an unavailable storage
// node is almost always a planned maintenance window measured in minutes, and
// a few minutes of honest query downtime beats an unknown quantity of quietly
// incomplete answers.
func TestStage9StrictQueryFailsOnNodeLoss(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	ns, c := newClusterWith(t, 4, netinsert.RoutingSpray, ds)

	q := &minilog.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}
	full, ssFull := c.Search(q)
	if ssFull.IsPartial() {
		t.Fatalf("healthy cluster reported a partial result: %s", ssFull)
	}

	ns.kill(2)
	defer ns.revive(2)

	got, ss := c.Search(q)

	if !ss.IsPartial() {
		t.Fatalf("a node is down and the query reported a COMPLETE result of %d rows\n"+
			"(the healthy cluster returns %d).\n\n"+
			"This is the failure mode stage 9 exists to prevent. Whatever the query\n"+
			"returns, SearchStats.NodesFailed must be non-zero when a node did not\n"+
			"answer. Stats: %s", len(got), len(full), ss)
	}
	if ss.NodesFailed != 1 {
		t.Errorf("NodesFailed=%d, want 1 (exactly one node was killed): %s", ss.NodesFailed, ss)
	}
	if len(got) >= len(full) {
		t.Errorf("a node holding %d of %d rows is down, yet the query returned %d rows.\n"+
			"Either the failure was not detected, or the strict path returned data it\n"+
			"should have discarded.", ns.rowsOnNode(2), len(full), len(got))
	}

	t.Logf("MEASUREMENT healthy: %d rows, complete", len(full))
	t.Logf("MEASUREMENT 1/4 nodes down, strict: %d rows, %s", len(got), ss)
}

// TestStage9PartialIsNeverSilent is the most important assertion in stage 9.
//
// Under AllowPartialResponse the query may return fewer rows. It must never
// return WRONG rows, and it must never claim to be complete. Every row it does
// return has to be a real row from the real answer -- a partial result is a
// subset, not an approximation.
func TestStage9PartialIsNeverSilent(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	ns, c := newClusterWith(t, 4, netinsert.RoutingSpray, ds)

	strict := &minilog.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}
	partialQ := &minilog.Query{
		MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64,
		AllowPartialResponse: true,
	}

	full, _ := c.Search(strict)
	fullSet := rowKeySet(full)

	for _, down := range [][]int{{0}, {1, 3}, {0, 1, 2}} {
		for _, i := range down {
			ns.kill(i)
		}

		got, ss := c.Search(partialQ)

		if !ss.IsPartial() {
			t.Fatalf("%d of 4 nodes are down and the result claims to be COMPLETE.\n"+
				"Stats: %s\n"+
				"Nothing above this layer can tell the difference between this and a\n"+
				"correct answer. That is the whole problem.", len(down), ss)
		}
		if ss.NodesFailed != len(down) {
			t.Errorf("%d nodes down, NodesFailed=%d", len(down), ss.NodesFailed)
		}

		// Subset, in order, no invented rows.
		if len(got) > len(full) {
			t.Fatalf("partial result has MORE rows (%d) than the complete one (%d)", len(got), len(full))
		}
		for i := range got {
			if _, ok := fullSet[rowKey(&got[i])]; !ok {
				t.Fatalf("partial result contains a row that is not in the complete answer.\n"+
					"A partial result must be a SUBSET. Returning anything else makes it\n"+
					"worse than an error.\n  row: %+v", got[i])
			}
		}
		if !isSorted(got) {
			t.Fatalf("partial result is not sorted by (StreamID, Timestamp)")
		}

		t.Logf("MEASUREMENT %d/4 nodes down: %d of %d rows (%.1f%%), %s",
			len(down), len(got), len(full), 100*float64(len(got))/float64(len(full)), ss)

		for _, i := range down {
			ns.revive(i)
		}
	}
}

// TestStage9AllNodesDownIsAnError checks the degenerate case.
//
// An empty result from a cluster where nothing is reachable is
// indistinguishable from an empty result from a healthy cluster that genuinely
// has no matching rows. Partial responses must not cover this case.
func TestStage9AllNodesDownIsAnError(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 50_000
	ds := gen.Generate(cfg)

	ns, c := newClusterWith(t, 3, netinsert.RoutingSpray, ds)
	for i := range ns.storages {
		ns.kill(i)
	}
	defer func() {
		for i := range ns.storages {
			ns.revive(i)
		}
	}()

	q := &minilog.Query{
		MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64,
		AllowPartialResponse: true,
	}
	got, ss := c.Search(q)

	if ss.NodesFailed != ss.NodesQueried {
		t.Fatalf("every node is down but NodesFailed=%d of NodesQueried=%d",
			ss.NodesFailed, ss.NodesQueried)
	}
	if len(got) != 0 {
		t.Fatalf("every node is down and the query returned %d rows", len(got))
	}
	if !ss.IsPartial() {
		t.Fatalf("every node is down and the result does not report as partial: %s\n"+
			"An empty answer from an empty cluster looks exactly like an empty answer\n"+
			"from a healthy one. The caller must be able to tell.", ss)
	}
	t.Logf("MEASUREMENT all nodes down: 0 rows, %s", ss)
}

// TestStage9ErrorResponseIsNotToleratedByPartial is the subtle one.
//
// A node that is DOWN is tolerable under AllowPartialResponse -- that is what
// the flag is for. A node that is UP and returning 500 is not. It almost
// always means misconfiguration or version skew, and hiding it behind a
// degraded-but-successful query is how a cluster becomes impossible to debug:
// the symptom is "results look a bit low sometimes" and the cause is a config
// error that has been shouting into a log nobody reads.
//
// Read getFirstError in app/vlstorage/netselect/netselect.go after you have
// written your own resolveErrors. The comment there makes exactly this
// argument.
func TestStage9ErrorResponseIsNotToleratedByPartial(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 50_000
	ds := gen.Generate(cfg)

	ns, c := newClusterWith(t, 4, netinsert.RoutingSpray, ds)

	q := &minilog.Query{
		MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64,
		AllowPartialResponse: true,
	}

	// Case 1: node 1 is DOWN. Tolerated -- rows come back, marked partial.
	ns.kill(1)
	gotDown, ssDown := c.Search(q)
	ns.revive(1)
	if !ssDown.IsPartial() || len(gotDown) == 0 {
		t.Fatalf("a downed node under AllowPartialResponse should degrade, not fail:\n"+
			"got %d rows, %s", len(gotDown), ssDown)
	}
	t.Logf("MEASUREMENT node down + partial:  %d rows, %s", len(gotDown), ssDown)

	// Case 2: node 1 is UP and returning 500. Must NOT be quietly absorbed.
	ns.fault(1)
	gotFault, ssFault := c.Search(q)
	ns.heal(1)

	t.Logf("MEASUREMENT node erroring + partial: %d rows, %s", len(gotFault), ssFault)

	if !ssFault.IsPartial() {
		t.Fatalf("a node answered every request with 500 and the result claims to be\n"+
			"COMPLETE: %s\n"+
			"An HTTP error response is not the same as an unreachable node. It means\n"+
			"the node is running and something about the request or its configuration\n"+
			"is wrong -- which will not fix itself, and which no amount of retrying\n"+
			"will route around.", ssFault)
	}
	if len(gotFault) != 0 && len(gotFault) >= len(gotDown)+ns.rowsOnNode(1) {
		t.Errorf("the erroring node's rows appear in the result (%d rows) -- a 500 was\n"+
			"treated as an empty success", len(gotFault))
	}
	t.Logf("NOTE Whether an erroring node should degrade or hard-fail the query is a\n" +
		"     policy call and this harness only requires that it is VISIBLE.\n" +
		"     VictoriaLogs hard-fails it even under partial responses. Decide which\n" +
		"     you want, implement it in resolveErrors, and write down why.")
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func rowKey(r *minilog.Row) string {
	s := sprintf("%d|%d", r.StreamID, r.Timestamp)
	for i := range r.Fields {
		s += "|" + r.Fields[i].Name + "=" + r.Fields[i].Value
	}
	return s
}

func rowKeySet(rows []minilog.Row) map[string]struct{} {
	out := make(map[string]struct{}, len(rows))
	for i := range rows {
		out[rowKey(&rows[i])] = struct{}{}
	}
	return out
}

func isSorted(rows []minilog.Row) bool {
	for i := 1; i < len(rows); i++ {
		if rows[i-1].StreamID > rows[i].StreamID {
			return false
		}
		if rows[i-1].StreamID == rows[i].StreamID && rows[i-1].Timestamp > rows[i].Timestamp {
			return false
		}
	}
	return true
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// silence unused-import complaints if a measurement is commented out locally.
var _ = netstorage.Config{}
