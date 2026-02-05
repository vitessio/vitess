//go:build !amd64 && !arm64

/*
Copyright 2023 The Vitess Authors.

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

package atomic2

import (
	"runtime"
	"sync/atomic"
)

type PointerAndUint64[T any] struct {
	spin atomic.Uint64
	p    *T
	u    uint64
}

func (x *PointerAndUint64[T]) Store(p *T, u uint64) {
	for !x.spin.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
	defer x.spin.Store(0)
	x.p = p
	x.u = u
}

func (x *PointerAndUint64[T]) Load() (*T, uint64) {
	for !x.spin.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
	defer x.spin.Store(0)
	return x.p, x.u
}

func (x *PointerAndUint64[T]) CompareAndSwap(oldp *T, oldu uint64, newp *T, newu uint64) bool {
	for !x.spin.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
	defer x.spin.Store(0)

	if x.p == oldp && x.u == oldu {
		x.p = newp
		x.u = newu
		return true
	}
	return false
}
