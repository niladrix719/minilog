package harness

// ---------------------------------------------------------------------------
// STAGE 6 MEASUREMENTS -- COMPLETE.
//
//	go test ./harness -run TestStage6 -v
//	go test ./harness -run FuzzStage6 -fuzz FuzzStage6RowRoundTrip -fuzztime 60s
//
// The wire format and the storage-node handler. No routing, no fan-out, no
// failure -- one client, one node, one socket.
//
// This stage looks like plumbing and is not. Everything after it assumes that
// a row put on the wire comes back identical and that a storage node behind
// HTTP is indistinguishable from one in the same process. If either is only
// mostly true, stages 7-10 will produce measurements that are wrong in ways
// that look like distributed-systems problems and are not.
// ---------------------------------------------------------------------------

import (
	"math"
	"strings"
	"testing"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/internalapi"
	"github.com/niladrix719/minilog/minilog"
)

// codecTestRows returns rows chosen to break a careless encoder.
//
// Every one of these is legal input that a real ingestion path can produce.
// None of them is exotic.
func codecTestRows() []minilog.Row {
	return []minilog.Row{
		// The ordinary case.
		{Timestamp: 1750000000123456789, StreamID: 0xdeadbeefcafe,
			Fields: []minilog.Field{{Name: "_msg", Value: "hello"}, {Name: "level", Value: "info"}}},

		// Zero stream id. A legal id, and the reason presence must be encoded
		// explicitly rather than by "0 means absent".
		{Timestamp: 1, StreamID: 0,
			Fields: []minilog.Field{{Name: "a", Value: "b"}}},

		// Maximum stream id: catches a varint that assumed 63 bits.
		{Timestamp: math.MaxInt64, StreamID: math.MaxUint64,
			Fields: []minilog.Field{{Name: "a", Value: "b"}}},

		// Timestamp zero and negative. Unix nanos before 1970 are legal and a
		// clock-skewed shipper will send them.
		{Timestamp: 0, StreamID: 7, Fields: []minilog.Field{{Name: "a", Value: "b"}}},
		{Timestamp: -1500000000000000000, StreamID: 7,
			Fields: []minilog.Field{{Name: "a", Value: "b"}}},

		// Empty value. Distinct from an absent field, and the pair must not
		// collapse into each other on the wire.
		{Timestamp: 5, StreamID: 8, Fields: []minilog.Field{{Name: "empty", Value: ""}}},

		// Bytes that are not valid UTF-8, and an embedded NUL. Log lines
		// contain these constantly and a length-prefixed encoding must not
		// care. An encoding that terminates strings on NUL truncates here.
		{Timestamp: 6, StreamID: 9, Fields: []minilog.Field{
			{Name: "binary", Value: "\x00\xff\xfe mid\x00dle"},
			{Name: "unicode", Value: "日本語 ☃ café"},
		}},

		// A long value: exercises multi-byte length prefixes.
		{Timestamp: 7, StreamID: 10, Fields: []minilog.Field{
			{Name: "big", Value: strings.Repeat("x", 200000)},
		}},

		// Many fields.
		{Timestamp: 8, StreamID: 11, Fields: manyFields(64)},
	}
}

func manyFields(n int) []minilog.Field {
	out := make([]minilog.Field, n)
	for i := range out {
		out[i] = minilog.Field{Name: sprintf("f%03d", i), Value: sprintf("v%d", i*7)}
	}
	return out
}

// TestStage6RowCodecRoundTrip is the first thing to make pass.
//
// The oracle is the input itself, so this measurement cannot be gamed: there
// is no way to write an encoder that agrees with a decoder on garbage and
// still reproduces the original row.
func TestStage6RowCodecRoundTrip(t *testing.T) {
	for i, want := range codecTestRows() {
		buf := internalapi.MarshalRow(nil, &want)

		var got minilog.Row
		tail, err := internalapi.UnmarshalRow(&got, buf)
		if err != nil {
			t.Fatalf("row %d: UnmarshalRow: %v", i, err)
		}
		if len(tail) != 0 {
			t.Fatalf("row %d: UnmarshalRow left %d trailing bytes -- the decoder is not\n"+
				"consuming exactly what the encoder wrote, which will desynchronise\n"+
				"the batch decoder on the very next row", i, len(tail))
		}
		if ok, msg := rowsEqual([]minilog.Row{got}, []minilog.Row{want}); !ok {
			t.Fatalf("row %d did not survive the round trip: %s", i, msg)
		}
	}

	// Marshal must append, not overwrite: the batch encoder depends on it.
	prefix := []byte("SENTINEL")
	r := codecTestRows()[0]
	out := internalapi.MarshalRow(prefix, &r)
	if !strings.HasPrefix(string(out), "SENTINEL") {
		t.Fatalf("MarshalRow overwrote dst instead of appending to it")
	}
}

// TestStage6BatchCodecRoundTrip round-trips a whole generated dataset.
func TestStage6BatchCodecRoundTrip(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 50_000
	ds := gen.Generate(cfg)
	minilog.SortRows(ds.Rows)

	buf := internalapi.MarshalRowBatch(nil, ds.Rows)

	got, tail, err := internalapi.UnmarshalRowBatch(nil, buf)
	if err != nil {
		t.Fatalf("UnmarshalRowBatch: %v", err)
	}
	if len(tail) != 0 {
		t.Fatalf("UnmarshalRowBatch left %d trailing bytes", len(tail))
	}
	if ok, msg := rowsEqual(got, ds.Rows); !ok {
		t.Fatalf("batch round trip: %s", msg)
	}

	// The classic bug: the decoder reuses one Row and appends it repeatedly,
	// so every entry in the result aliases the last row decoded. rowsEqual
	// above catches it, but say so explicitly because the fix is not obvious.
	if len(got) > 1 && got[0].Timestamp == got[len(got)-1].Timestamp &&
		ds.Rows[0].Timestamp != ds.Rows[len(ds.Rows)-1].Timestamp {
		t.Fatalf("every decoded row is identical -- UnmarshalRowBatch is appending the\n" +
			"same reused Row over and over. The scratch row must be copied (fields\n" +
			"included) before it goes into the output slice.")
	}

	t.Logf("MEASUREMENT batch: %d rows -> %d wire bytes", len(ds.Rows), len(buf))
}

// TestStage6WireBytesPerRow measures the format against raw JSON.
//
// Same denominator as the stage 1 compression ratio, so the two numbers are
// directly comparable: stage 1 told you what your on-disk format buys, this
// tells you what your wire format buys.
func TestStage6WireBytesPerRow(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)
	minilog.SortRows(ds.Rows)

	buf := internalapi.MarshalRowBatch(nil, ds.Rows)

	wire := float64(len(buf))
	logical := float64(ds.LogicalBytes)
	perRow := wire / float64(len(ds.Rows))

	t.Logf("MEASUREMENT ==================== WIRE FORMAT ====================")
	t.Logf("MEASUREMENT rows:              %d", len(ds.Rows))
	t.Logf("MEASUREMENT logical (JSON):    %.1f MiB  (%.1f B/row)", logical/(1<<20), logical/float64(len(ds.Rows)))
	t.Logf("MEASUREMENT wire:              %.1f MiB  (%.1f B/row)", wire/(1<<20), perRow)
	t.Logf("MEASUREMENT ratio vs JSON:     %.2fx", logical/wire)
	t.Logf("MEASUREMENT =============================================================")
	t.Logf("NOTE Two levers, and you should know which one you pulled:\n" +
		"     (a) timestamps. A raw int64 of unix nanos costs 9 bytes as a varint\n" +
		"         every single time, because the top bits are always set. Zigzag\n" +
		"         or delta-encode and watch this number move.\n" +
		"     (b) field names. Every row here carries the same 11 names. Shipping\n" +
		"         them per-row spends roughly 60 B/row on strings the peer already\n" +
		"         knows. A per-batch name dictionary reclaims it in the format;\n" +
		"         zstd reclaims most of it in the transport. Measure both, then\n" +
		"         decide -- 'let the compressor handle it' is a defensible answer\n" +
		"         and you should be able to say by how much.")

	if logical/wire < 1.5 {
		t.Errorf("wire format is only %.2fx smaller than raw JSON (%.1f B/row).\n"+
			"A binary length-prefixed format should comfortably beat JSON on this\n"+
			"data. Are you marshaling with encoding/json, or writing field names\n"+
			"and timestamps at full width?", logical/wire, perRow)
	}
}

// TestStage6QueryCodecRoundTrip checks every Query field survives.
//
// A dropped query field is the nastiest bug in this stage, because the result
// is not an error -- it is a plausible answer to a question nobody asked. A
// lost Limit returns too many rows. A lost AllowPartialResponse turns a
// degraded query into a hard failure, or worse, the reverse.
func TestStage6QueryCodecRoundTrip(t *testing.T) {
	sid := uint64(0)
	bigSid := uint64(math.MaxUint64)

	cases := []*minilog.Query{
		{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64},
		{MinTimestamp: -5, MaxTimestamp: 5, StreamID: &sid},
		{MinTimestamp: 1, MaxTimestamp: 2, StreamID: &bigSid},
		{MinTimestamp: 1, MaxTimestamp: 2, Limit: 100},
		{MinTimestamp: 1, MaxTimestamp: 2, AllowPartialResponse: true},
		{
			MinTimestamp: 1750000000000000000, MaxTimestamp: 1750000003600000000,
			StreamID:             &bigSid,
			Limit:                42,
			AllowPartialResponse: true,
			Filters: []minilog.Filter{
				{Column: "_msg", Token: "error"},
				{Column: "level", Token: ""},
				{Column: "", Token: "weird"},
				{Column: "unicode", Token: "日本語"},
			},
		},
	}

	for i, want := range cases {
		buf := internalapi.MarshalQuery(nil, want)

		var got minilog.Query
		tail, err := internalapi.UnmarshalQuery(&got, buf)
		if err != nil {
			t.Fatalf("query %d: UnmarshalQuery: %v", i, err)
		}
		if len(tail) != 0 {
			t.Fatalf("query %d: %d trailing bytes after decode", i, len(tail))
		}

		if got.MinTimestamp != want.MinTimestamp || got.MaxTimestamp != want.MaxTimestamp {
			t.Fatalf("query %d: time range %d..%d != %d..%d",
				i, got.MinTimestamp, got.MaxTimestamp, want.MinTimestamp, want.MaxTimestamp)
		}
		if (got.StreamID == nil) != (want.StreamID == nil) {
			t.Fatalf("query %d: StreamID presence lost (got nil=%v, want nil=%v).\n"+
				"Encode presence with an explicit byte. 0 is a legal stream id, so\n"+
				"'0 means absent' silently turns a single-stream query into a\n"+
				"whole-cluster scan.", i, got.StreamID == nil, want.StreamID == nil)
		}
		if got.StreamID != nil && *got.StreamID != *want.StreamID {
			t.Fatalf("query %d: StreamID %d != %d", i, *got.StreamID, *want.StreamID)
		}
		if got.Limit != want.Limit {
			t.Fatalf("query %d: Limit %d != %d -- a dropped limit returns too many rows,\n"+
				"which no test of the ANSWER will notice", i, got.Limit, want.Limit)
		}
		if got.AllowPartialResponse != want.AllowPartialResponse {
			t.Fatalf("query %d: AllowPartialResponse %v != %v -- stage 9 depends on this",
				i, got.AllowPartialResponse, want.AllowPartialResponse)
		}
		if len(got.Filters) != len(want.Filters) {
			t.Fatalf("query %d: %d filters != %d", i, len(got.Filters), len(want.Filters))
		}
		for j := range got.Filters {
			if got.Filters[j] != want.Filters[j] {
				t.Fatalf("query %d filter %d: %+v != %+v", i, j, got.Filters[j], want.Filters[j])
			}
		}
	}
}

// TestStage6DecoderRejectsGarbage checks the decoder errors rather than
// panicking or over-allocating on corrupt input.
//
// These bytes come off a socket. "The peer is a slightly older version of
// ourselves" is the normal case during a rolling upgrade, not an exotic one,
// and a storage node that panics on a malformed batch is a storage node that
// one bad client can take down.
func TestStage6DecoderRejectsGarbage(t *testing.T) {
	r := codecTestRows()[0]
	good := internalapi.MarshalRow(nil, &r)

	bad := map[string][]byte{
		"empty":              {},
		"truncated-half":     good[:len(good)/2],
		"truncated-one-byte": good[:len(good)-1],
		"single-zero":        {0x00},
		"huge-length-prefix": {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
		"all-ones":           {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	}

	for name, src := range bad {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("UnmarshalRow PANICKED on %s input: %v\n\n"+
						"(If this is the 'TODO stage 6' panic, implement the decoder first.)\n"+
						"Otherwise: this input arrives over a network. Every length prefix\n"+
						"in it is chosen by the peer. Check remaining length before every\n"+
						"read and before every allocation, and return an error.", name, p)
				}
			}()
			var got minilog.Row
			if _, err := internalapi.UnmarshalRow(&got, src); err == nil {
				t.Fatalf("UnmarshalRow accepted %s input without an error", name)
			}
		})
	}
}

// FuzzStage6RowRoundTrip is the general version of the round-trip test.
//
//	go test ./harness -run FuzzStage6RowRoundTrip -fuzz FuzzStage6RowRoundTrip -fuzztime 60s
//
// Let it run for a minute after the table test is green. It finds the cases
// you did not think of, which for a length-prefixed codec is usually an empty
// string somewhere you assumed there would not be one.
func FuzzStage6RowRoundTrip(f *testing.F) {
	f.Add(uint64(1), int64(0), "a", "b", "c", "d")
	f.Add(uint64(0), int64(-1), "", "", "", "")
	f.Add(uint64(math.MaxUint64), int64(math.MaxInt64), "_msg", "\x00\xff", "z", "日本語")

	f.Fuzz(func(t *testing.T, sid uint64, ts int64, n1, v1, n2, v2 string) {
		// Fields must be sorted by name and names must be distinct -- both are
		// preconditions of the format, not properties it should repair.
		if n1 >= n2 {
			t.Skip()
		}
		want := minilog.Row{
			Timestamp: ts, StreamID: sid,
			Fields: []minilog.Field{{Name: n1, Value: v1}, {Name: n2, Value: v2}},
		}

		buf := internalapi.MarshalRow(nil, &want)
		var got minilog.Row
		tail, err := internalapi.UnmarshalRow(&got, buf)
		if err != nil {
			t.Fatalf("round trip failed: %v (row %+v)", err, want)
		}
		if len(tail) != 0 {
			t.Fatalf("%d trailing bytes (row %+v)", len(tail), want)
		}
		if ok, msg := rowsEqual([]minilog.Row{got}, []minilog.Row{want}); !ok {
			t.Fatalf("%s (row %+v)", msg, want)
		}
	})
}

// TestStage6ProtocolVersionIsChecked asserts a version mismatch is rejected
// before the body is interpreted.
//
// This is the mechanism that makes rolling upgrades survivable, and it is
// worth being blunt about what it prevents: without it, a v1 node receiving v2
// bytes does not error. It decodes them as v1, gets plausible-looking
// nonsense, and writes it to disk. The corruption is permanent and silent and
// it happened during a routine deploy.
func TestStage6ProtocolVersionIsChecked(t *testing.T) {
	ns := newNodeSet(t, 1)

	r := codecTestRows()[0]
	body := internalapi.MarshalRowBatch(nil, []minilog.Row{r})

	for _, version := range []string{"", "v0", "v99", "V1", "v1 "} {
		url := sprintf("http://%s%s?%s=%s", ns.addrs[0], internalapi.InsertPath,
			internalapi.VersionArg, version)
		code, respBody := httpPost(t, url, body)
		if code == 200 || code == 204 {
			t.Fatalf("storage node ACCEPTED an insert with version=%q (it speaks %q).\n"+
				"Check the version before reading the body, and reject with 400.",
				version, internalapi.ProtocolVersion)
		}
		if code != 400 {
			t.Errorf("version=%q rejected with status %d; want 400 (the REQUEST is\n"+
				"wrong, so it must never be retried or re-routed -- stage 9 depends on\n"+
				"400 and 5xx meaning different things). Body: %s", version, code, respBody)
		}
	}

	// And the matching version must work.
	url := sprintf("http://%s%s?%s=%s", ns.addrs[0], internalapi.InsertPath,
		internalapi.VersionArg, internalapi.ProtocolVersion)
	if code, respBody := httpPost(t, url, body); code/100 != 2 {
		t.Fatalf("storage node REJECTED the correct version %q with status %d: %s",
			internalapi.ProtocolVersion, code, respBody)
	}

	t.Logf("MEASUREMENT version check: 5 bad versions rejected with 400, %q accepted",
		internalapi.ProtocolVersion)
}

// TestStage6SeamTransparency is the headline stage 6 measurement.
//
// A storage node reached over HTTP must be indistinguishable from a
// minilog.Storage in the same process. Same queries, same rows, same order.
//
// If this passes, the seam holds, and every stage after it is free to assume
// that a difference between local and cluster results is a distributed-systems
// bug rather than a serialisation one. That assumption is worth a great deal
// when stage 8 disagrees with brute force and you have to guess where to look.
func TestStage6SeamTransparency(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	ds := gen.Generate(cfg)

	// The reference: the local engine, as built in stages 1-4.
	local := newStorage(t, &minilog.Config{ShardsCount: 1})
	ingest(t, local, ds, 10_000)
	runConformance(t, local, ds, "local")

	// The same data, the same queries, through a socket.
	ns := newNodeSet(t, 1)
	remote := newDirectNode(ns.addrs[0])
	ingest(t, remote, ds, 10_000)
	runConformance(t, remote, ds, "over HTTP")

	// And the two must agree with each other, not just with the oracle.
	for i, q := range conformanceQueries(ds) {
		gotLocal, ssLocal := local.Search(q)
		gotRemote, ssRemote := remote.Search(q)
		if ok, msg := rowsEqual(gotRemote, gotLocal); !ok {
			t.Fatalf("query %d: local and remote disagree: %s", i, msg)
		}
		if i == 0 {
			t.Logf("MEASUREMENT full scan local:  %s", ssLocal)
			t.Logf("MEASUREMENT full scan remote: %s", ssRemote)
		}
	}

	t.Logf("MEASUREMENT %d conformance queries: local == over-HTTP, both == brute force",
		len(conformanceQueries(ds)))
	t.Logf("NOTE The seam holds. From here on, a difference between a local result\n" +
		"     and a cluster result is a routing, fan-out or merge bug -- not a\n" +
		"     codec bug. That is worth more than it sounds like when stage 8\n" +
		"     disagrees with brute force and you have to decide where to look.")
}
