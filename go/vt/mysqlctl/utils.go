package mysqlctl

import (
	"code.google.com/p/vitess/go/relog"
)

type MapFunc func(interface{}) (interface{}, error)

// ConcurrentMap calls fun on every element of todo using concurrency
// goroutines and returns the aggregated results (in the same order as
// they were provided in todo). If any call to fun returns an error,
// its result will be nil, and ConcurrentMap will also return a
// non-nil error.
func ConcurrentMap(concurrency int, todo []interface{}, fun MapFunc) ([]interface{}, error) {
	n := len(todo)
	results := make([]interface{}, n)
	errors := make(chan error)
	work := make(chan int, n)

	for i := 0; i < n; i++ {
		work <- i
	}
	close(work)

	for j := 0; j < concurrency; j++ {
		go func() {
			for i := range work {
				item := todo[i]
				result, err := fun(item)
				if err != nil {
					result = nil
				}
				results[i] = result
				errors <- err
			}
		}()
	}
	var err error

	for i := 0; i < n; i++ {
		if e := <-errors; e != nil {
			if err != nil {
				relog.Error("multiple errors, this one happened but it won't be returned: %v", err)
			}
			err = e
		}
	}
	return results, err
}
