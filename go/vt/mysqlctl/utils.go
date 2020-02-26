/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysqlctl

import (
	"vitess.io/vitess/go/vt/log"
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
				log.Errorf("multiple errors, this one happened but it won't be returned: %v", err)
			}
			err = e
		}
	}
	return err
}
