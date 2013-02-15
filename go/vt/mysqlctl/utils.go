package mysqlctl

import (
	"code.google.com/p/vitess/go/relog"
)

type MapFunc func(index int) error

// ConcurrentMap applies fun in a concurrent manner on integers from 0
// to n-1 (they are assumed to be indexes of some slice containing
// items to be processed). The first error returned by a fun
// application will returned (subsequent errors will only be
// logged). It will use concurrency goroutines.
func ConcurrentMap(concurrency, n int, fun MapFunc) error {
	errors := make(chan error)
	work := make(chan int, n)

	for i := 0; i < n; i++ {
		work <- i
	}
	close(work)

	for j := 0; j < concurrency; j++ {
		go func() {
			for i := range work {
				errors <- fun(i)
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
	return err
}
