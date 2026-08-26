package harness

// COMPLETE -- shared helpers for the stage measurements.

import (
	"testing"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
)

// newStorage opens a storage in a temp dir that is cleaned up automatically.
func newStorage(t *testing.T, cfg *minilog.Config) *minilog.Storage {
	t.Helper()
	if cfg == nil {
		cfg = &minilog.Config{ShardsCount: 1}
	}
	minilog.ResetMetrics()
	s := minilog.MustOpenStorage(t.TempDir(), cfg)
	t.Cleanup(s.MustClose)
	return s
}

// ingest adds a dataset to storage in batches and forces a flush.
func ingest(t *testing.T, s *minilog.Storage, ds *gen.Dataset, batchSize int) {
	t.Helper()
	rows := ds.Rows
	for len(rows) > 0 {
		n := batchSize
		if n > len(rows) {
			n = len(rows)
		}
		s.MustAddRows(rows[:n])
		rows = rows[n:]
	}
	s.MustForceFlush()
}

// bruteForce is the oracle: a full linear scan using Query.MatchesRow.
//
// Every indexed query result is compared against this. It shares no code with
// the block-skipping path on purpose -- if it did, a bug in skipping could
// hide behind an identical bug in verification.
func bruteForce(rows []minilog.Row, q *minilog.Query) []minilog.Row {
	var out []minilog.Row
	for i := range rows {
		if q.MatchesRow(&rows[i]) {
			out = append(out, rows[i])
		}
	}
	minilog.SortRows(out)
	return out
}

// rowsEqual compares two row slices exactly: same length, same order, same
// timestamps, stream ids, and field name/value pairs.
func rowsEqual(a, b []minilog.Row) (bool, string) {
	if len(a) != len(b) {
		return false, sprintf("length mismatch: got %d, want %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Timestamp != b[i].Timestamp {
			return false, sprintf("row %d: timestamp %d != %d", i, a[i].Timestamp, b[i].Timestamp)
		}
		if a[i].StreamID != b[i].StreamID {
			return false, sprintf("row %d: streamID %d != %d", i, a[i].StreamID, b[i].StreamID)
		}
		if len(a[i].Fields) != len(b[i].Fields) {
			return false, sprintf("row %d: field count %d != %d", i, len(a[i].Fields), len(b[i].Fields))
		}
		for j := range a[i].Fields {
			if a[i].Fields[j] != b[i].Fields[j] {
				return false, sprintf("row %d field %d: %v != %v", i, j, a[i].Fields[j], b[i].Fields[j])
			}
		}
	}
	return true, ""
}

func sprintf(format string, args ...any) string {
	return fmtSprintf(format, args...)
}
