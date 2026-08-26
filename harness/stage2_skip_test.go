package harness

// ---------------------------------------------------------------------------
// STAGE 2 MEASUREMENTS, part 2 -- COMPLETE.
//
//	go test ./harness -run TestStage2Skip -v
//
// This measures the actual payoff of the whole index-free design: how much
// work you avoid on a selective query. It is the number that justifies
// VictoriaLogs not maintaining an inverted index over field values.
// ---------------------------------------------------------------------------

import (
	"math"
	"testing"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
)

// TestStage2SkipRareVsCommon is the headline stage 2 measurement.
//
// A rare token should eliminate nearly every block via bloom filters.
// A common token should eliminate nearly none.
// The gap between those two numbers IS the value of bloom filtering.
func TestStage2SkipRareVsCommon(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 500_000
	cfg.RareTokenInjections = 25
	ds := gen.Generate(cfg)

	s := newStorage(t, &minilog.Config{ShardsCount: 1})
	ingest(t, s, ds, 10_000)

	fullRange := func(f minilog.Filter) *minilog.Query {
		return &minilog.Query{
			MinTimestamp: math.MinInt64,
			MaxTimestamp: math.MaxInt64,
			Filters:      []minilog.Filter{f},
		}
	}

	// --- rare token -------------------------------------------------------
	qRare := fullRange(minilog.Filter{Column: "_msg", Token: gen.RareToken})
	gotRare, ssRare := s.Search(qRare)

	wantRare := bruteForce(ds.Rows, qRare)
	if ok, msg := rowsEqual(gotRare, wantRare); !ok {
		t.Fatalf("rare-token query disagrees with brute-force scan: %s\n"+
			"You are skipping a block you should not skip. This is the bug class\n"+
			"bloom filters make possible and it is silent in production.", msg)
	}
	if len(gotRare) != ds.RareTokenRows {
		t.Fatalf("rare-token query returned %d rows, generator injected %d", len(gotRare), ds.RareTokenRows)
	}

	t.Logf("MEASUREMENT rare token  %q -> %d rows", gen.RareToken, len(gotRare))
	t.Logf("MEASUREMENT   %s", ssRare)
	t.Logf("MEASUREMENT   skip ratio: %.2f%%", ssRare.SkipRatio()*100)

	// --- common token -----------------------------------------------------
	qCommon := fullRange(minilog.Filter{Column: "_msg", Token: gen.CommonToken})
	gotCommon, ssCommon := s.Search(qCommon)

	if len(gotCommon) != len(ds.Rows) {
		t.Fatalf("common-token query returned %d rows, want all %d "+
			"(gen.CommonToken is injected into every message)", len(gotCommon), len(ds.Rows))
	}

	t.Logf("MEASUREMENT common token %q -> %d rows", gen.CommonToken, len(gotCommon))
	t.Logf("MEASUREMENT   %s", ssCommon)
	t.Logf("MEASUREMENT   skip ratio: %.2f%%", ssCommon.SkipRatio()*100)

	// --- the assertions ---------------------------------------------------
	if ssRare.BlocksTotal == 0 {
		t.Fatalf("BlocksTotal is 0 -- you are not bumping SearchStats.BlocksTotal in part.searchBlockHeaders")
	}
	if ssRare.BlocksSkippedByBloom == 0 {
		t.Fatalf("BlocksSkippedByBloom is 0 on a token present in ~%d of %d rows.\n"+
			"Either you are not consulting bloom filters in searchBlockHeaders,\n"+
			"or you are not bumping the counter. Both make stage 2 meaningless.",
			ds.RareTokenRows, len(ds.Rows))
	}

	// NOTE: threshold is 0.85, not the 0.95 you'd expect from pure block count
	// (500_000 rows / MaxRowsPerBlock). Background merging (stage 3) runs on
	// every flush by default, consolidating many small per-flush blocks into
	// fewer, larger ones -- which lowers the achievable skip ratio for a fixed
	// number of rare-token rows, with zero effect on correctness (0 false
	// positives either way; verified by disabling merging and re-measuring:
	// 98.44% with merging off, 93.75% with the default threshold=8).
	if ssRare.SkipRatio() < 0.85 {
		t.Errorf("rare-token skip ratio is only %.2f%%, expected >85%%.\n"+
			"With ~%d matching rows spread over %d blocks, almost every block\n"+
			"should be eliminated. Check that you consult the bloom BEFORE\n"+
			"decoding values, and that you build the bloom over _msg tokens.",
			ssRare.SkipRatio()*100, ds.RareTokenRows, ssRare.BlocksTotal)
	}

	if ssCommon.SkipRatio() > 0.10 {
		t.Errorf("common-token skip ratio is %.2f%%, expected near 0%%.\n"+
			"A token present in every row must not be skippable. If you are\n"+
			"skipping blocks here you have a false-negative bug and are silently\n"+
			"dropping results.", ssCommon.SkipRatio()*100)
	}

	speedup := float64(ssCommon.BlocksScanned) / float64(max(ssRare.BlocksScanned, 1))
	t.Logf("MEASUREMENT ===> bloom filtering avoided %.0fx the block decodes on the selective query",
		speedup)
}

// TestStage2SkipFalsePositivesInSitu measures the FP rate through the real
// query path rather than against the Bloom type directly.
//
// This catches a different bug class than the unit-level FP test: filters that
// are correct in isolation but are built over the wrong token set, sized from
// the wrong n, or read back from the wrong offset in bloom.bin.
func TestStage2SkipFalsePositivesInSitu(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	ds := gen.Generate(cfg)

	s := newStorage(t, &minilog.Config{ShardsCount: 1})
	ingest(t, s, ds, 10_000)

	k := float64(minilog.BloomHashesCount)
	theory := math.Pow(1-math.Exp(-k/float64(minilog.BloomBitsPerItem)), k)

	var total minilog.SearchStats
	absent := gen.AbsentTokens(300, 99)
	for _, tok := range absent {
		q := &minilog.Query{
			MinTimestamp: math.MinInt64,
			MaxTimestamp: math.MaxInt64,
			Filters:      []minilog.Filter{{Column: "_msg", Token: tok}},
		}
		rows, ss := s.Search(q)
		if len(rows) != 0 {
			t.Fatalf("query for guaranteed-absent token %q returned %d rows", tok, len(rows))
		}
		total.Merge(ss)
	}

	if total.BlocksTotal == 0 {
		t.Fatalf("BlocksTotal is 0 across %d queries -- counters are not being bumped", len(absent))
	}

	measured := float64(total.BloomMaybe) / float64(total.BlocksTotal)
	t.Logf("MEASUREMENT in-situ FP over %d absent-token queries:", len(absent))
	t.Logf("MEASUREMENT   blocks considered: %d", total.BlocksTotal)
	t.Logf("MEASUREMENT   bloom said maybe:  %d", total.BloomMaybe)
	t.Logf("MEASUREMENT   measured FP rate:  %.4f%%   (unit-level theory: %.4f%%)",
		measured*100, theory*100)
	t.Logf("NOTE in-situ FP is expected to be somewhat HIGHER than the unit-level\n"+
		"     theory: each block's filter holds the tokens of a whole column block,\n"+
		"     and short/common tokens from the tokenizer inflate the load factor.\n"+
		"     What matters is the order of magnitude, not the exact value.")

	if total.BloomMaybe == total.BlocksTotal {
		t.Errorf("every block said maybe for every absent token -- bloom filters are\n" +
			"having no effect at all. Check that searchBlockHeaders actually calls\n" +
			"Contains and returns early on false.")
	}
	if measured > 0.05 {
		t.Errorf("in-situ FP rate %.2f%% is implausibly high (>5%%).\n"+
			"Likely: you are sizing filters from a deduped token count but inserting\n"+
			"a non-deduped set, or several columns share one filter.", measured*100)
	}
}
