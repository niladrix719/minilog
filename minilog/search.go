package minilog

// ---------------------------------------------------------------------------
// COMPLETE (types) / STAGE 1+2 (matching logic) -- the Query type is fixed so
// the harnesses can construct queries. Filter.Matches is yours.
//
// Deliberately tiny query model: a time range plus an AND of (column, token)
// filters. No parser, no pipes, no aggregation. That is enough to exercise
// every skipping mechanism and nothing more -- adding a query language would
// double the work and teach you nothing new about storage.
// ---------------------------------------------------------------------------

import "slices"

// Query selects rows by time range and an AND of token filters.
type Query struct {
	// MinTimestamp and MaxTimestamp are inclusive unix nanoseconds.
	MinTimestamp int64
	MaxTimestamp int64

	// StreamID, if non-nil, restricts the query to a single stream.
	// This is the cheapest possible filter: it is answered from the block
	// header alone, and from the metaindex before that.
	StreamID *uint64

	// Filters are ANDed together. Empty means "match every row in range".
	Filters []Filter
}

// Filter matches rows where Column contains Token as a word token.
//
// Token semantics, matching what your Tokenize produces: the filter matches
// if Token appears in the column value's token set. So Token "error" matches
// the value "connection error: timeout" but not "errors".
//
// This is exactly the shape of filter a bloom filter can answer negatively,
// which is why it is the one modelled here.
type Filter struct {
	Column string
	Token  string
}

// Matches reports whether a decoded value satisfies f.
//
// This is the ground-truth check, run only on blocks that survived bloom
// filtering. It must be independent of the bloom path: if a bug made this
// agree with a broken bloom filter, the false-positive measurement would be
// meaningless.
func (f *Filter) Matches(value string) bool {
	tokens := Tokenize(nil, value)
	return slices.Contains(tokens, f.Token)
}

// MatchesRow reports whether every filter in q matches the row, and the row
// is within the time range.
//
// The stage 1 and stage 3 harnesses use this as a brute-force oracle: they
// run a full scan with MatchesRow and compare against the result of the real
// indexed query path. Any divergence is a bug in the skipping logic -- a
// block was skipped that should not have been.
//
// Keep this dead simple. It is checked code, not fast code.
func (q *Query) MatchesRow(r *Row) bool {
	if (q.StreamID != nil && r.StreamID != *q.StreamID) || r.Timestamp > q.MaxTimestamp || r.Timestamp < q.MinTimestamp {
		return false
	}
	for _, f := range q.Filters {
		value, isPresent := r.GetField(f.Column)
		if !isPresent || !f.Matches(value) {
			return false
		}
	}
	return true
}

// Columns returns the set of column names q reads.
//
// Used by mustReadBlockRows to decode only the needed columns -- this is
// projection pushdown in its simplest form, and it is the same idea as
// pipe.updateNeededFields in VictoriaLogs, just without the backward pass
// over a pipe chain.
func (q *Query) Columns() []string {
	seen := make(map[string]bool)
	var cols []string
	for _, f := range q.Filters {
		if !seen[f.Column] {
			seen[f.Column] = true
			cols = append(cols, f.Column)
		}
	}
	return cols
}
