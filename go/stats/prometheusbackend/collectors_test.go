package prometheusbackend

import (
	"testing"

	"vitess.io/vitess/go/stats"
)

func getStats() map[string]int64 {
	stats := make(map[string]int64)
	stats["table1.plan"] = 1
	stats["table2.plan"] = 2
	// Errors are expected to be logged from collectors.go when
	// adding this value (issue #5599).  Test will succeed, as intended.
	stats["table3.dot.plan"] = 3
	return stats
}

func TestCollector(t *testing.T) {
	c := stats.NewCountersFuncWithMultiLabels("Name", "description", []string{"Table", "Plan"}, getStats)
	c.Counts()
}
