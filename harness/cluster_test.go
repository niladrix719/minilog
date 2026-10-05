package harness

// ---------------------------------------------------------------------------
// COMPLETE -- shared scaffolding for the stage 6-10 measurements.
//
// A cluster in these tests is real: real HTTP over real loopback sockets, real
// serialisation, real concurrency. The only thing faked is the process
// boundary -- every storage node is an in-process minilog.Storage behind its
// own listener. That keeps `go test` fast and keeps -race useful, and it costs
// exactly one thing: it cannot test process death. That is what
// cmd/chaostest is for.
//
// Two kinds of failure can be injected, and keeping them distinct is the whole
// of stage 9:
//
//	kill(i)   the node stops answering at the transport level. No HTTP
//	          response ever arrives. This is "the node is down".
//	fault(i)  the node answers every request with 500. This is "the node is
//	          up and misconfigured", and it must NOT be tolerated by partial
//	          responses.
// ---------------------------------------------------------------------------

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niladrix719/minilog/agent"
	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/internalapi"
	"github.com/niladrix719/minilog/minilog"
	"github.com/niladrix719/minilog/netinsert"
	"github.com/niladrix719/minilog/netselect"
	"github.com/niladrix719/minilog/netstorage"
)

// ---------------------------------------------------------------------------
// gatedListener -- transport-level kill switch
// ---------------------------------------------------------------------------

// gatedListener accepts connections only while its gate is open. While closed
// it accepts and immediately drops them, which the client sees as a reset or
// EOF -- indistinguishable from a machine that has gone away, and instant to
// toggle in both directions.
//
// The alternative (closing the listener and re-listening on the same port) is
// at the mercy of TIME_WAIT and makes tests flaky on exactly the CI box you
// cannot reproduce on.
type gatedListener struct {
	net.Listener
	open atomic.Bool
}

func (l *gatedListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.open.Load() {
			return c, nil
		}
		_ = c.Close()
	}
}

// ---------------------------------------------------------------------------
// nodeServer -- application-level fault injection
// ---------------------------------------------------------------------------

type nodeServer struct {
	h      http.Handler
	faulty atomic.Bool

	// Stage 11 fault modes. Both apply to InsertPath only, so flushes and
	// queries keep working while inserts misbehave.
	//
	// reject400: the node answers every insert with 400. A client that
	// retries this forever is stuck forever.
	reject400 atomic.Bool

	// storeThenFail: the node STORES the rows and then answers 503, as if the
	// response were lost on the way back. The client cannot tell this from a
	// node that never stored anything, so it retries -- and the rows are
	// stored twice. This is the at-least-once case, made deterministic.
	storeThenFail atomic.Bool

	requests atomic.Int64
}

func (n *nodeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n.requests.Add(1)
	if n.faulty.Load() {
		http.Error(w, "injected fault: this node is up but broken", http.StatusInternalServerError)
		return
	}
	if r.URL.Path == internalapi.InsertPath {
		if n.reject400.Load() {
			http.Error(w, "injected fault: this node rejects every insert", http.StatusBadRequest)
			return
		}
		if n.storeThenFail.Load() {
			rec := httptest.NewRecorder()
			n.h.ServeHTTP(rec, r)
			http.Error(w, "injected fault: stored, but the response was lost", http.StatusServiceUnavailable)
			return
		}
	}
	n.h.ServeHTTP(w, r)
}

// ---------------------------------------------------------------------------
// nodeSet -- N storage nodes behind N listeners
// ---------------------------------------------------------------------------

type nodeSet struct {
	t         *testing.T
	storages  []*minilog.Storage
	servers   []*nodeServer
	listeners []*gatedListener
	addrs     []string
}

// newNodeSet starts n storage nodes, each with its own temp dir and listener.
func newNodeSet(t *testing.T, n int) *nodeSet {
	t.Helper()
	minilog.ResetMetrics()

	ns := &nodeSet{t: t}
	for i := 0; i < n; i++ {
		st := minilog.MustOpenStorage(t.TempDir(), &minilog.Config{ShardsCount: 1})

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		gl := &gatedListener{Listener: ln}
		gl.open.Store(true)

		srv := &nodeServer{h: internalapi.NewStorageNodeHandler(st)}
		httpSrv := &http.Server{Handler: srv, ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = httpSrv.Serve(gl) }()

		ns.storages = append(ns.storages, st)
		ns.servers = append(ns.servers, srv)
		ns.listeners = append(ns.listeners, gl)
		ns.addrs = append(ns.addrs, ln.Addr().String())

		t.Cleanup(func() {
			_ = httpSrv.Close()
			st.MustClose()
		})
	}
	return ns
}

func (ns *nodeSet) kill(i int)   { ns.listeners[i].open.Store(false) }
func (ns *nodeSet) revive(i int) { ns.listeners[i].open.Store(true) }
func (ns *nodeSet) fault(i int)  { ns.servers[i].faulty.Store(true) }
func (ns *nodeSet) heal(i int)   { ns.servers[i].faulty.Store(false) }

// Stage 11 insert-path fault modes. See nodeServer.
func (ns *nodeSet) rejectInserts(i int, on bool) { ns.servers[i].reject400.Store(on) }
func (ns *nodeSet) storeThenFail(i int, on bool) { ns.servers[i].storeThenFail.Store(on) }

// flushAll flushes every node's local storage directly, bypassing HTTP.
//
// Used to establish ground truth without depending on the code under test.
func (ns *nodeSet) flushAll() {
	for _, st := range ns.storages {
		st.MustForceFlush()
	}
}

// rowsOnNode returns how many rows node i actually holds.
//
// Reads the node's local storage directly -- no network, no netselect, no
// netinsert. This is the independent oracle for every routing and fan-out
// measurement: it shares no code with the thing it is checking.
func (ns *nodeSet) rowsOnNode(i int) int {
	rows, _ := ns.storages[i].Search(&minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
	})
	return len(rows)
}

// rowCounts returns rows actually stored per node.
func (ns *nodeSet) rowCounts() []int {
	out := make([]int, len(ns.storages))
	for i := range ns.storages {
		out[i] = ns.rowsOnNode(i)
	}
	return out
}

// nodesMatching returns how many nodes hold at least one row matching q.
//
// The fan-out tax measurement: compare this against len(ns.storages) to see
// how many nodes a query had to open that had nothing to contribute.
func (ns *nodeSet) nodesMatching(q *minilog.Query) int {
	n := 0
	for i := range ns.storages {
		if rows, _ := ns.storages[i].Search(q); len(rows) > 0 {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// clients
// ---------------------------------------------------------------------------

// insertClient returns a netinsert client for this node set.
func (ns *nodeSet) insertClient(cfg netinsert.Config) *netinsert.Storage {
	cfg.Addrs = ns.addrs
	s := netinsert.NewStorage(&cfg)
	ns.t.Cleanup(s.MustClose)
	return s
}

// agentFor returns an agent forwarding to the given node indexes, with its
// queues under dataPath. Pass the same dataPath twice to test restart.
//
// Backoff is set low so an outage test measures the agent, not the timer.
// The caller closes it; a restart test must close the first one before
// opening the second on the same dir.
func (ns *nodeSet) agentFor(dataPath string, cfg agent.Config, nodes ...int) *agent.Agent {
	for _, i := range nodes {
		cfg.Addrs = append(cfg.Addrs, ns.addrs[i])
	}
	cfg.DataPath = dataPath
	if cfg.RetryMinInterval == 0 {
		cfg.RetryMinInterval = 20 * time.Millisecond
	}
	if cfg.RetryMaxInterval == 0 {
		cfg.RetryMaxInterval = 200 * time.Millisecond
	}
	return agent.NewAgent(&cfg)
}

// cluster returns the full insert+select facade over this node set.
func (ns *nodeSet) cluster(cfg netstorage.Config) *netstorage.Storage {
	cfg.Addrs = ns.addrs
	s := netstorage.NewStorage(&cfg)
	ns.t.Cleanup(s.MustClose)
	return s
}

// selectClient returns a netselect client for this node set.
func (ns *nodeSet) selectClient(cfg netselect.Config) *netselect.Storage {
	cfg.Addrs = ns.addrs
	s := netselect.NewStorage(&cfg)
	ns.t.Cleanup(s.MustClose)
	return s
}

// ---------------------------------------------------------------------------
// directNode -- the stage 6 client
// ---------------------------------------------------------------------------

// directNode is a minilog.LogStorage that talks internalapi to exactly one
// storage node over HTTP.
//
// It exists so stage 6 can be measured before netinsert or netselect exist.
// It is the smallest possible thing that crosses the seam: encode, POST,
// decode. If a directNode is indistinguishable from a local minilog.Storage,
// the codec and the handler are correct, and every later stage is building on
// something known-good.
type directNode struct {
	addr string
	c    *http.Client
}

func newDirectNode(addr string) *directNode {
	return &directNode{addr: addr, c: &http.Client{Timeout: 30 * time.Second}}
}

func (d *directNode) urlFor(path string) string {
	return fmt.Sprintf("http://%s%s?%s=%s", d.addr, path,
		internalapi.VersionArg, url.QueryEscape(internalapi.ProtocolVersion))
}

func (d *directNode) MustAddRows(rows []minilog.Row) {
	body := internalapi.MarshalRowBatch(nil, rows)
	resp, err := d.c.Post(d.urlFor(internalapi.InsertPath), "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		panic(fmt.Sprintf("directNode insert: %v", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("directNode insert: status %d: %s", resp.StatusCode, b))
	}
}

func (d *directNode) Search(q *minilog.Query) ([]minilog.Row, *minilog.SearchStats) {
	body := internalapi.MarshalQuery(nil, q)
	resp, err := d.c.Post(d.urlFor(internalapi.SelectPath), "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		panic(fmt.Sprintf("directNode select: %v", err))
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(fmt.Sprintf("directNode select: read body: %v", err))
	}
	if resp.StatusCode/100 != 2 {
		panic(fmt.Sprintf("directNode select: status %d: %s", resp.StatusCode, respBody))
	}
	rows, ss, err := internalapi.UnmarshalQueryResponse(respBody)
	if err != nil {
		panic(fmt.Sprintf("directNode select: decode response: %v", err))
	}
	return rows, ss
}

func (d *directNode) MustForceFlush() {
	resp, err := d.c.Get(d.urlFor(internalapi.ForceFlushPath))
	if err != nil {
		panic(fmt.Sprintf("directNode flush: %v", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("directNode flush: status %d: %s", resp.StatusCode, b))
	}
}

func (d *directNode) MustClose() {}

var _ minilog.LogStorage = (*directNode)(nil)

// ---------------------------------------------------------------------------
// conformance -- the shared definition of "behaves like storage"
// ---------------------------------------------------------------------------

// conformanceQueries returns the queries every LogStorage must answer
// identically, whatever is behind it.
//
// Deliberately covers each skipping mechanism you built: time-range pruning,
// stream filtering, a rare token (bloom says no almost everywhere), a common
// token (bloom says maybe everywhere), a token that does not exist at all, and
// a conjunction. If an implementation of the seam is wrong, one of these
// finds it.
func conformanceQueries(ds *gen.Dataset) []*minilog.Query {
	full := func(fs ...minilog.Filter) *minilog.Query {
		return &minilog.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64, Filters: fs}
	}
	lo, hi := ds.Rows[0].Timestamp, ds.Rows[len(ds.Rows)-1].Timestamp
	mid := lo + (hi-lo)/2
	sid := ds.Streams[3].ID

	return []*minilog.Query{
		full(),
		{MinTimestamp: mid, MaxTimestamp: hi},
		{MinTimestamp: lo, MaxTimestamp: mid, StreamID: &sid},
		full(minilog.Filter{Column: "_msg", Token: gen.RareToken}),
		full(minilog.Filter{Column: "_msg", Token: gen.CommonToken}),
		full(minilog.Filter{Column: "_msg", Token: gen.AbsentTokens(1, 7)[0]}),
		full(minilog.Filter{Column: "level", Token: "error"}),
		full(minilog.Filter{Column: "level", Token: "error"}, minilog.Filter{Column: "method", Token: "GET"}),
	}
}

// runConformance asserts s answers every conformance query exactly as a
// brute-force scan of ds does.
//
// bruteForce shares no code with anything under test -- not the local engine,
// not the codec, not the network. That independence is the reason a green
// conformance run means something.
func runConformance(t *testing.T, s minilog.LogStorage, ds *gen.Dataset, label string) {
	t.Helper()
	for i, q := range conformanceQueries(ds) {
		got, ss := s.Search(q)
		want := q.ApplyLimit(bruteForce(ds.Rows, q))
		if ok, msg := rowsEqual(got, want); !ok {
			t.Fatalf("%s: conformance query %d disagrees with brute force: %s\n"+
				"  query: %+v\n"+
				"  stats: %s\n"+
				"A LogStorage that is not interchangeable with the local engine is\n"+
				"not a LogStorage. Every later stage assumes this holds.",
				label, i, msg, q, ss)
		}
	}
}

// ---------------------------------------------------------------------------
// skew -- Zipfian stream distribution, for the stage 7 routing table
// ---------------------------------------------------------------------------

// skewStreams returns a copy of ds whose rows are redistributed across the
// same stream set according to a Zipf distribution with exponent s.
//
// s = 0 gives the uniform distribution gen produces. Larger s concentrates
// more rows in fewer streams; real log volume looks roughly like s = 1.0-1.3,
// where a handful of chatty services dominate everything.
//
// IMPORTANT: only Row.StreamID is changed, so the rows' stream-ish FIELDS
// (service, pod, ...) no longer agree with their stream id. That is fine for
// the routing measurements, which look only at StreamID, and it is NOT fine
// for correctness measurements. Do not feed a skewed dataset to
// runConformance.
func skewStreams(ds *gen.Dataset, s float64, seed int64) *gen.Dataset {
	rng := rand.New(rand.NewSource(seed))
	n := len(ds.Streams)

	// Zipf weights over stream ranks.
	weights := make([]float64, n)
	total := 0.0
	for i := range weights {
		weights[i] = 1 / math.Pow(float64(i+1), s)
		total += weights[i]
	}
	cum := make([]float64, n)
	acc := 0.0
	for i := range weights {
		acc += weights[i] / total
		cum[i] = acc
	}

	out := &gen.Dataset{
		Rows:          make([]minilog.Row, len(ds.Rows)),
		Streams:       ds.Streams,
		RareTokenRows: ds.RareTokenRows,
		LogicalBytes:  ds.LogicalBytes,
	}
	copy(out.Rows, ds.Rows)
	for i := range out.Rows {
		x := rng.Float64()
		idx := 0
		for idx < n-1 && x > cum[idx] {
			idx++
		}
		out.Rows[i].StreamID = ds.Streams[idx].ID
	}
	return out
}

// ---------------------------------------------------------------------------
// balance
// ---------------------------------------------------------------------------

// balanceRatio is max node load divided by mean node load. 1.0 is perfect.
func balanceRatio(counts []int) float64 {
	if len(counts) == 0 {
		return 0
	}
	maxN, sum := 0, 0
	for _, c := range counts {
		if c > maxN {
			maxN = c
		}
		sum += c
	}
	mean := float64(sum) / float64(len(counts))
	if mean == 0 {
		return 0
	}
	return float64(maxN) / mean
}

// predictedBalanceRatio is the closed-form expectation for hashing `streams`
// streams uniformly onto `nodes` nodes, assuming streams carry equal volume.
//
// Balls into bins: with m balls in n bins and m >> n log n, the maximum load
// concentrates around m/n + sqrt(2 (m/n) ln n). Dividing by the mean m/n:
//
//	max/mean ~= 1 + sqrt(2 ln n / (m/n))
//
// This is asymptotic, so it is a shape to check against, not a constant to
// assert to three decimal places. What it predicts firmly is the DIRECTION and
// the ORDER: balance improves as streams-per-node grows, and it improves as
// the square root, which is why doubling your stream count barely helps.
//
// A measured ratio well BELOW this is the interesting failure. Genuine hashing
// cannot beat the theory; something that does is not random -- most likely the
// stream ids and the node count share a factor, or the router is quietly
// round-robining.
func predictedBalanceRatio(streams, nodes int) float64 {
	if nodes <= 1 {
		return 1
	}
	perNode := float64(streams) / float64(nodes)
	if perNode <= 0 {
		return 0
	}
	return 1 + math.Sqrt(2*math.Log(float64(nodes))/perNode)
}

// ---------------------------------------------------------------------------
// http helpers
// ---------------------------------------------------------------------------

// httpPost posts body to url and returns the status code and response body.
func httpPost(t *testing.T, url string, body []byte) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}
