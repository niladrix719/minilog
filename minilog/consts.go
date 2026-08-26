package minilog

// Tunables. These mirror lib/logstorage/consts.go in VictoriaLogs, scaled down
// where it makes the exercise faster to iterate on.
//
// Several harness measurements sweep these values. Keep them as vars (not
// consts) where a harness needs to change them at runtime.

const (
	// MaxRowsPerBlock is the maximum number of rows in a single block.
	//
	// VictoriaLogs uses 8*1024*1024. We use a much smaller value so that a
	// modest test dataset still produces thousands of blocks -- otherwise the
	// bloom-skipping measurements have nothing to skip.
	MaxRowsPerBlock = 8 * 1024

	// MaxColumnsPerBlock caps how many distinct field names a block may hold.
	MaxColumnsPerBlock = 256

	// MaxDictLen is the maximum number of distinct values for dict encoding.
	//
	// Must not exceed 255: the dict length is marshaled into a single byte.
	// This is why VictoriaLogs caps it at 8 -- see the exercise README.
	MaxDictLen = 8

	// MaxDictSizeBytes is the maximum total size of all dict keys.
	MaxDictSizeBytes = 256

	// PartFormatVersion is written into metadata.json. Bump it when you change
	// the on-disk layout so that old parts fail loudly instead of silently
	// decoding as garbage.
	PartFormatVersion = 1
)

// Tunables the harnesses sweep. These are vars, not consts, on purpose.
var (
	// InmemoryPartMaxRows is the row count at which an in-memory part is
	// flushed to disk as a "small" part.
	InmemoryPartMaxRows = 64 * 1024

	// SmallPartsMergeThreshold is how many small parts may accumulate before
	// they are merged into a big part.
	//
	// harness/stage3_writeamp_test.go sweeps this to plot the
	// write-amplification vs read-amplification curve.
	SmallPartsMergeThreshold = 8
)

// nsecsPerDay is the number of nanoseconds in a day. Partitions are named by
// (unixNanos / nsecsPerDay), same as VictoriaLogs.
const nsecsPerDay = 24 * 60 * 60 * 1e9
