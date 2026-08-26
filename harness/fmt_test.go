package harness

import "fmt"

// COMPLETE -- trivial indirection so helpers_test.go stays free of fmt noise.
func fmtSprintf(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}
