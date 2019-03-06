/*
Copyright 2019 The Vitess Authors

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

package bucketpool

import (
	"math"
	"sync"
)

type sizedPool struct {
	size int
	pool sync.Pool
}

func newSizedPool(size int) *sizedPool {
	return &sizedPool{
		size: size,
		pool: sync.Pool{
			New: func() interface{} { return makeSlicePointer(size) },
		},
	}
}

// Pool is actually multiple pools which store buffers of specific size.
// i.e. it can be three pools which return buffers 32K, 64K and 128K.
type Pool struct {
	minSize int
	maxSize int
	pools   []*sizedPool
}

// New returns Pool which has buckets from minSize to maxSize.
// Buckets increase with the power of two, i.e with multiplier 2: [2b, 4b, 16b, ... , 1024b]
// Last pool will always be capped to maxSize.
func New(minSize, maxSize int) *Pool {
	if maxSize < minSize {
		panic("maxSize can't be less than minSize")
	}
	const multiplier = 2
	var pools []*sizedPool
	curSize := minSize
	for curSize < maxSize {
		pools = append(pools, newSizedPool(curSize))
		curSize *= multiplier
	}
	pools = append(pools, newSizedPool(maxSize))
	return &Pool{
		minSize: minSize,
		maxSize: maxSize,
		pools:   pools,
	}
}

func (p *Pool) findPool(size int) *sizedPool {
	if size > p.maxSize {
		return nil
	}
	idx := int(math.Ceil(math.Log2(float64(size) / float64(p.minSize))))
	if idx < 0 {
		idx = 0
	}
	if idx > len(p.pools)-1 {
		return nil
	}
	return p.pools[idx]
}

// Get returns pointer to []byte which has len size.
// If there is no bucket with buffers >= size, slice will be allocated.
func (p *Pool) Get(size int) *[]byte {
	sp := p.findPool(size)
	if sp == nil {
		return makeSlicePointer(size)
	}
	buf := sp.pool.Get().(*[]byte)
	*buf = (*buf)[:size]
	return buf
}

// Put returns pointer to slice to some bucket. Discards slice for which there is no bucket
func (p *Pool) Put(b *[]byte) {
	sp := p.findPool(cap(*b))
	if sp == nil {
		return
	}
	*b = (*b)[:cap(*b)]
	sp.pool.Put(b)
}

func makeSlicePointer(size int) *[]byte {
	data := make([]byte, size)
	return &data
}
