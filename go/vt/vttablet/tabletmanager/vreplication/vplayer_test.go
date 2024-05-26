/*
Copyright 2024 The Vitess Authors.

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

package vreplication

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
)

func TestStallHandler(t *testing.T) {
	lctx := utils.LeakCheckContext(t)
	tme := time.Duration(90 * time.Second)
	ctx, cancel := context.WithTimeout(lctx, tme)
	defer cancel()

	concurrency := 10000
	dl := time.Duration(10 * time.Second)

	tests := []struct {
		name string
		f    func()
	}{
		{
			name: "Random concurrency",
			f: func() {
				ch := make(chan error)
				sh := newStallHandler(dl, ch)
				defer sh.stopTimer() // This should always be called in a defer from where it's created
				wg := sync.WaitGroup{}
				for i := 0; i < concurrency; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
						var err error
						action := rand.Intn(2)
						if action == 0 {
							err = sh.startTimer()
						} else {
							err = sh.stopTimer()
						}
						require.NoError(t, err)
						select {
						case e := <-ch:
							require.FailNow(t, "unexpected error", "error: %v", e)
						case <-ctx.Done():
						default:
						}
					}()
				}
				wg.Wait()
			},
		},
		{
			name: "All stops",
			f: func() {
				ch := make(chan error)
				sh := newStallHandler(dl, ch)
				defer sh.stopTimer() // This should always be called in a defer from where it's created
				wg := sync.WaitGroup{}
				for i := 0; i < concurrency; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						err := sh.stopTimer()
						require.NoError(t, err)
						select {
						case e := <-ch:
							require.FailNow(t, "unexpected error", "error: %v", e)
						case <-ctx.Done():
						default:
						}
					}()
				}
				wg.Wait()
			},
		},
		{
			name: "All starts",
			f: func() {
				ch := make(chan error)
				sh := newStallHandler(dl, ch)
				defer sh.stopTimer() // This should always be called in a defer from where it's created
				wg := sync.WaitGroup{}
				for i := 0; i < concurrency; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						err := sh.startTimer()
						require.NoError(t, err)
						select {
						case e := <-ch:
							require.FailNow(t, "unexpected error", "error: %v", e)
						case <-ctx.Done():
						default:
						}
					}()
				}
				wg.Wait()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.f()
		})
	}
}
