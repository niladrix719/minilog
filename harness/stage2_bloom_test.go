package harness

// ---------------------------------------------------------------------------
// STAGE 2 MEASUREMENTS -- COMPLETE. This is the centrepiece of the exercise.
//
//	go test ./harness -run TestStage2Bloom -v
//
// The false-positive test compares your measured rate against the closed-form
// prediction
//
//	FP = (1 - e^(-k/bitsPerItem))^k
//
// which for k=6, bitsPerItem=16 is about 0.094%. The prediction is computed
// here from your exported constants and knows nothing about your
// implementation, so you cannot satisfy it with a self-consistent bug.
//
// Read the diagnostics on failure: the DIRECTION of the error tells you which
// bug you have.
// ---------------------------------------------------------------------------

import (
	"fmt"
	"math"
	"testing"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
)

// TestStage2BloomNoFalseNegatives is the non-negotiable correctness property.
//
// A bloom filter may say "maybe" when the answer is no. It may NEVER say "no"
// when the answer is yes -- that would silently drop matching log lines from
// query results, which is the worst possible bug in a log store.
func TestStage2BloomNoFalseNegatives(t *testing.T) {
	for _, n := range []int{1, 2, 10, 1000, 100_000} {
		tokens := gen.AbsentTokens(n, 7)
		var bf minilog.Bloom
		bf.Init(tokens)

		for i, tok := range tokens {
			if !bf.Contains(tok) {
				t.Fatalf("FALSE NEGATIVE with n=%d at token %d (%q).\n"+
					"A bloom filter must never miss an inserted item.\n"+
					"Most likely cause: Init and Contains do not derive their k\n"+
					"hashes identically, or Init sized the bit array using a\n"+
					"different n than it inserted.", n, i, tok)
			}
		}
	}
}

// TestStage2BloomFalsePositiveRate measures the FP rate against theory.
func TestStage2BloomFalsePositiveRate(t *testing.T) {
	const (
		nInserted = 100_000
		nProbes   = 2_000_000
	)

	k := float64(minilog.BloomHashesCount)
	bitsPerItem := float64(minilog.BloomBitsPerItem)
	theory := math.Pow(1-math.Exp(-k/bitsPerItem), k)

	inserted := gen.AbsentTokens(nInserted, 11)
	var bf minilog.Bloom
	bf.Init(inserted)

	// Probes drawn from a different seed, so they cannot collide with the
	// inserted set. (AbsentTokens embeds 128 bits of entropy; accidental
	// overlap is not a practical concern.)
	probes := gen.AbsentTokens(nProbes, 22)
	insertedSet := make(map[string]struct{}, nInserted)
	for _, tok := range inserted {
		insertedSet[tok] = struct{}{}
	}

	hits := 0
	checked := 0
	for _, tok := range probes {
		if _, ok := insertedSet[tok]; ok {
			continue // genuinely present, not a false positive
		}
		checked++
		if bf.Contains(tok) {
			hits++
		}
	}

	measured := float64(hits) / float64(checked)

	// Expected size: n items * bitsPerItem bits, in bytes.
	wantBytes := float64(nInserted) * bitsPerItem / 8
	gotBytes := float64(bf.SizeBytes())

	t.Logf("MEASUREMENT bloom k=%d bitsPerItem=%d n=%d probes=%d",
		minilog.BloomHashesCount, minilog.BloomBitsPerItem, nInserted, checked)
	t.Logf("MEASUREMENT   theoretical FP rate: %.4f%%", theory*100)
	t.Logf("MEASUREMENT   measured    FP rate: %.4f%%  (%d hits)", measured*100, hits)
	t.Logf("MEASUREMENT   filter size: %.1f KiB (expected ~%.1f KiB, %.2f bytes/item)",
		gotBytes/1024, wantBytes/1024, gotBytes/float64(nInserted))

	switch {
	case hits == 0:
		t.Fatalf("measured FP rate is EXACTLY ZERO over %d probes, but theory predicts %.4f%% (~%.0f hits).\n"+
			"A real bloom filter cannot do this. Almost certainly Contains() is not\n"+
			"actually reading the bit array -- e.g. it returns a value derived from a\n"+
			"set/map you kept on the side, or it always returns false.",
			checked, theory*100, theory*float64(checked))

	case measured < theory*0.4:
		t.Errorf("measured FP rate %.4f%% is far BELOW theory %.4f%%.\n"+
			"Too good is a bug, not a win. Likely causes:\n"+
			"  - Init sized the bit array from a token count larger than the number\n"+
			"    actually inserted (did you forget to dedupe before sizing, or size\n"+
			"    from cap() instead of len()?). Check bytes/item above: it should be\n"+
			"    ~%.2f, and yours is %.2f.\n"+
			"  - you inserted fewer items than you think.",
			measured*100, theory*100, bitsPerItem/8, gotBytes/float64(nInserted))

	case measured > theory*2.5:
		t.Errorf("measured FP rate %.4f%% is far ABOVE theory %.4f%%.\n"+
			"Likely causes, in order of frequency:\n"+
			"  - your k hash values are correlated. If you built them as\n"+
			"    xxhash(token+string(i)) or h>>i, they are not independent.\n"+
			"    Use Kirsch-Mitzenmacher: h1 = uint32(h), h2 = uint32(h>>32),\n"+
			"    then hash_i = h1 + i*h2.\n"+
			"  - the bit array is smaller than n*bitsPerItem (bytes/item is %.2f,\n"+
			"    expected ~%.2f).\n"+
			"  - you are reducing the hash to a bit index with a mask that discards\n"+
			"    entropy, or with %% over a non-prime that shares factors with h2.",
			measured*100, theory*100, gotBytes/float64(nInserted), bitsPerItem/8)

	default:
		t.Logf("PASS: measured FP rate is within the expected band of theory. "+
			"Your bloom filter is real. (ratio measured/theory = %.2f)", measured/theory)
	}

	if gotBytes > wantBytes*1.6 {
		t.Errorf("filter is %.2f bytes/item, expected ~%.2f. You are wasting space; "+
			"check your rounding up to whole uint64 words.",
			gotBytes/float64(nInserted), bitsPerItem/8)
	}
}

// TestStage2BloomMarshal checks serialisation round-trips and rejects garbage.
func TestStage2BloomMarshal(t *testing.T) {
	tokens := gen.AbsentTokens(5000, 33)
	var bf minilog.Bloom
	bf.Init(tokens)

	buf := bf.Marshal(nil)

	var bf2 minilog.Bloom
	if err := bf2.Unmarshal(buf); err != nil {
		t.Fatalf("Unmarshal of a freshly marshaled filter failed: %v", err)
	}
	for _, tok := range tokens {
		if !bf2.Contains(tok) {
			t.Fatalf("token %q lost across Marshal/Unmarshal", tok)
		}
	}
	if bf2.SizeBytes() != bf.SizeBytes() {
		t.Fatalf("size changed across round-trip: %d -> %d", bf.SizeBytes(), bf2.SizeBytes())
	}

	// Truncated input must error, never panic. The crash test will hand you
	// exactly this.
	for n := 0; n < len(buf); n += max(1, len(buf)/50) {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Unmarshal PANICKED on truncated input of len %d: %v.\n"+
						"Malformed on-disk data must produce an error, not a panic.", n, r)
				}
			}()
			var bf3 minilog.Bloom
			_ = bf3.Unmarshal(buf[:n])
		}()
	}
}

// TestStage2Tokenizer checks the tokenizer contract the bloom path depends on.
func TestStage2Tokenizer(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"hello", []string{"hello"}},
		{"hello world", []string{"hello", "world"}},
		{"GET /api/v1/users 200", []string{"GET", "api", "v1", "users", "200"}},
		{"connection error: timeout after 30s", []string{"connection", "error", "timeout", "after", "30s"}},
		{"...", nil},
		{"a-b_c", []string{"a", "b", "c"}},
	}

	for _, c := range cases {
		got := minilog.Tokenize(nil, c.in)
		if len(got) != len(c.want) {
			t.Errorf("Tokenize(%q) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			// Case-insensitive compare: lowercasing is a legitimate design
			// choice, so the test does not force one.
			if !equalFold(got[i], c.want[i]) {
				t.Errorf("Tokenize(%q) = %v, want %v", c.in, got, c.want)
				break
			}
		}
	}

	// Dedup is required: it is what makes bloom sizing correct.
	values := repeatCycle([]string{"info", "warn", "error"}, 10_000)
	toks := minilog.TokenizeValues(nil, values)
	if len(toks) != 3 {
		t.Errorf("TokenizeValues over 10000 values with 3 distinct tokens returned %d tokens, want 3.\n"+
			"Without dedup your bloom filters will be sized from n=10000 instead of n=3,\n"+
			"which wastes ~2.5KiB per column per block and makes the FP measurement lie.",
			len(toks))
	}

	// The filter's ground-truth matcher must agree with the tokenizer, or the
	// bloom "maybe" can never be resolved correctly.
	f := minilog.Filter{Column: "_msg", Token: "error"}
	if !f.Matches("connection error: timeout") {
		t.Errorf("Filter.Matches disagrees with Tokenize: %q should match token %q", "connection error: timeout", "error")
	}
	if f.Matches("connection errors: timeout") {
		t.Errorf("Filter.Matches is doing substring matching, not token matching: %q must not match token %q",
			"connection errors: timeout", "error")
	}
}

func equalFold(a, b string) bool {
	return fmt.Sprintf("%s", lower(a)) == lower(b)
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}
