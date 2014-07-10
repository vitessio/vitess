package stats

import (
	"fmt"
	"strings"
)

// MultiTracker is a CountTracker that tracks counts grouping them by
// more than one dimension.
type MultiTracker interface {
	CountTracker
	Labels() []string
}

// CounterForDimension returns a CountTracker for the provided
// dimension. It will panic if the dimension isn't a legal label for
// mt.
func CounterForDimension(mt MultiTracker, dimension string) CountTracker {
	for i, lab := range mt.Labels() {
		if lab == dimension {
			return CountersFunc(func() map[string]int64 {
				result := make(map[string]int64)
				for k, v := range mt.Counts() {
					if k == "All" {
						result[k] = v
						continue
					}
					result[strings.Split(k, ".")[i]] += v
				}
				return result
			})
		}
	}
	panic(fmt.Sprintf("label %v is not one of %v", dimension, mt.Labels))

}
