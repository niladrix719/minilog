package harness

// ---------------------------------------------------------------------------
// STAGE 1 MEASUREMENTS -- COMPLETE. Do not weaken these to make them pass.
//
//	go test ./harness -run TestStage1 -v
//
// What they prove:
//   - your varints, strings and delta-encoded timestamps round-trip exactly
//   - your per-column encoder never loses information (including the
//     leading-zero trap)
//   - a full write -> read cycle through real files returns byte-identical rows
//   - and it reports your compression ratio, which is a number, not a pass/fail
// ---------------------------------------------------------------------------

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
)

func TestStage1Varint(t *testing.T) {
	values := []uint64{
		0, 1, 2, 127, 128, 129, 255, 256, 16383, 16384,
		1 << 20, 1 << 31, 1 << 32, 1<<63 - 1, math.MaxUint64,
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10000; i++ {
		values = append(values, rng.Uint64()>>uint(rng.Intn(64)))
	}

	for _, want := range values {
		buf := minilog.MarshalVarUint64(nil, want)
		got, tail, err := minilog.UnmarshalVarUint64(buf)
		if err != nil {
			t.Fatalf("UnmarshalVarUint64(%d): unexpected error: %v", want, err)
		}
		if got != want {
			t.Fatalf("varint round-trip: got %d, want %d", got, want)
		}
		if len(tail) != 0 {
			t.Fatalf("varint round-trip for %d: %d trailing bytes", want, len(tail))
		}
	}

	// Truncated input must error, not panic and not loop.
	full := minilog.MarshalVarUint64(nil, math.MaxUint64)
	for n := 0; n < len(full); n++ {
		if _, _, err := minilog.UnmarshalVarUint64(full[:n]); err == nil {
			t.Fatalf("UnmarshalVarUint64 accepted truncated input of len %d", n)
		}
	}
}

func TestStage1Strings(t *testing.T) {
	inputs := []string{"", "a", "hello world", "üñïçødé", string(make([]byte, 100000))}
	var buf []byte
	for _, s := range inputs {
		buf = minilog.MarshalString(buf, s)
	}
	tail := buf
	for _, want := range inputs {
		got, rest, err := minilog.UnmarshalString(tail)
		if err != nil {
			t.Fatalf("UnmarshalString: unexpected error: %v", err)
		}
		if got != want {
			t.Fatalf("string round-trip: got %q, want %q", got, want)
		}
		tail = rest
	}
	if len(tail) != 0 {
		t.Fatalf("%d trailing bytes after decoding all strings", len(tail))
	}
}

func TestStage1Timestamps(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	base := int64(1750000000 * 1e9)

	for _, count := range []int{1, 2, 100, 8192} {
		ts := make([]int64, count)
		cur := base
		for i := range ts {
			cur += rng.Int63n(5 * 1e6) // up to 5ms apart, ascending
			ts[i] = cur
		}

		buf := minilog.MarshalTimestamps(nil, ts)
		got, err := minilog.UnmarshalTimestamps(nil, buf, count)
		if err != nil {
			t.Fatalf("UnmarshalTimestamps: unexpected error: %v", err)
		}
		if len(got) != count {
			t.Fatalf("timestamp count: got %d, want %d", len(got), count)
		}
		for i := range ts {
			if got[i] != ts[i] {
				t.Fatalf("timestamp %d: got %d, want %d", i, got[i], ts[i])
			}
		}

		if count == 8192 {
			bytesPerTS := float64(len(buf)) / float64(count)
			t.Logf("MEASUREMENT delta-encoded timestamps: %.2f bytes/row (raw int64 = 8.00)", bytesPerTS)
			if bytesPerTS >= 8 {
				t.Errorf("delta encoding is not helping: %.2f bytes/row >= 8. "+
					"Are you writing full int64s instead of varint deltas?", bytesPerTS)
			}
		}
	}
}

// TestStage1EncodeValues checks the per-column encoder, including the traps.
func TestStage1EncodeValues(t *testing.T) {
	cases := []struct {
		name     string
		values   []string
		wantType minilog.ValueType
	}{
		{
			name:     "low cardinality picks dict",
			values:   repeatCycle([]string{"info", "warn", "error"}, 8192),
			wantType: minilog.ValueTypeDict,
		},
		{
			name:     "plain integers pick uint64",
			values:   []string{"0", "1", "42", "999999", "18446744073709551615"},
			wantType: minilog.ValueTypeUint64,
		},
		{
			name:     "high cardinality strings fall back to string",
			values:   uniqueStrings(1000),
			wantType: minilog.ValueTypeString,
		},
		{
			// THE TRAP. "007" parses as 7, but encoding it as uint64 loses the
			// leading zeros and breaks round-trip. Your encoder must not take
			// the bait.
			name:     "leading zeros must not become uint64",
			values:   []string{"007", "042", "100"},
			wantType: minilog.ValueTypeString,
		},
		{
			name:     "empty values",
			values:   []string{"", "", ""},
			wantType: minilog.ValueTypeDict,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			buf, vt, dict := minilog.EncodeValues(nil, c.values)

			got, err := minilog.DecodeValues(nil, buf, vt, dict, len(c.values))
			if err != nil {
				t.Fatalf("DecodeValues: unexpected error: %v", err)
			}
			if len(got) != len(c.values) {
				t.Fatalf("value count: got %d, want %d", len(got), len(c.values))
			}
			for i := range c.values {
				if got[i] != c.values[i] {
					t.Fatalf("value %d: got %q, want %q (encoded as %s)", i, got[i], c.values[i], vt)
				}
			}

			if vt != c.wantType {
				t.Errorf("chose %s encoding, expected %s", vt, c.wantType)
			}

			raw := 0
			for _, v := range c.values {
				raw += len(v) + 1
			}
			t.Logf("MEASUREMENT %s: %s, %d bytes for %d values (raw %d, ratio %.2fx)",
				c.name, vt, len(buf), len(c.values), raw, float64(raw)/float64(max(len(buf), 1)))
		})
	}
}

// TestStage1RoundTrip is the headline stage 1 measurement: a full ingest and
// full scan through real files, asserting exact equality, and reporting how
// much smaller the result is than raw JSON.
func TestStage1RoundTrip(t *testing.T) {
	cfg := gen.DefaultConfig()
	ds := gen.Generate(cfg)

	s := newStorage(t, &minilog.Config{ShardsCount: 1})
	ingest(t, s, ds, 10_000)

	q := &minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
	}
	got, ss := s.Search(q)

	want := make([]minilog.Row, len(ds.Rows))
	copy(want, ds.Rows)
	minilog.SortRows(want)

	if ok, msg := rowsEqual(got, want); !ok {
		t.Fatalf("full scan is not byte-identical to input: %s", msg)
	}

	ratio := minilog.CompressionRatio()
	t.Logf("MEASUREMENT rows=%d logical=%.1fMiB onDisk=%.1fMiB ratio=%.2fx",
		len(ds.Rows),
		float64(ds.LogicalBytes)/(1<<20),
		float64(minilog.Metrics.BytesLiveOnDisk.Load())/(1<<20),
		ratio)
	t.Logf("MEASUREMENT search stats: %s", ss)

	if ratio < 5 {
		t.Errorf("compression ratio %.2fx is below the 5x target.\n"+
			"Things to check, in order of typical impact:\n"+
			"  - are you extracting const columns? (pod/service/region/namespace\n"+
			"    are constant within a stream's block -- that is 4 of 11 fields)\n"+
			"  - is `level`/`status`/`method` picking dict encoding?\n"+
			"  - are timestamps delta-encoded?\n"+
			"  - are you writing fixed-width ints anywhere you could varint?",
			ratio)
	}
}

func repeatCycle(vals []string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = vals[i%len(vals)]
	}
	return out
}

func uniqueStrings(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("trace-%016x-%d", uint64(i)*0x9e3779b97f4a7c15, i)
	}
	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
