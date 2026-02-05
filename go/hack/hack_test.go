//go:build gc && !wasm

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

package hack

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteToString(t *testing.T) {
	v1 := []byte("1234")
	s := String(v1)
	assert.Equal(t, "1234", s)

	v1 = []byte("")
	s = String(v1)
	assert.Equal(t, "", s)

	v1 = nil
	s = String(v1)
	assert.Equal(t, "", s)
}

func TestStringToByte(t *testing.T) {
	s := "1234"
	b := StringBytes(s)
	assert.Equal(t, []byte("1234"), b)

	s = ""
	b = StringBytes(s)
	assert.Nil(t, b)
}

func testMapSize[K comparable, V any](t *testing.T, gen func(i int) (K, V)) {
	for _, size := range []int{16 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)

		m := make(map[K]V, size)
		for i := 0; i < size; i++ {
			k, v := gen(i)
			m[k] = v
		}

		runtime.GC()
		runtime.ReadMemStats(&after)

		heapDiff := after.HeapAlloc - before.HeapAlloc
		calcSize := RuntimeMapSize(m)

		assert.InEpsilonf(t, heapDiff, calcSize, 0.1, "%Tx%v heapDiff = %v, calcSize = %v", m, size, heapDiff, calcSize)
		runtime.KeepAlive(m)
	}
}

func TestMapSize(t *testing.T) {
	testMapSize(t, func(i int) (int, int) {
		return i, i
	})

	testMapSize(t, func(i int) (uint32, uint32) {
		return uint32(i), uint32(i)
	})

	testMapSize(t, func(i int) ([32]uint32, uint32) {
		return [32]uint32{0: uint32(i)}, uint32(i)
	})

	testMapSize(t, func(i int) (uint32, [32]uint32) {
		return uint32(i), [32]uint32{0: uint32(i)}
	})

	testMapSize(t, func(i int) ([32]uint32, [32]uint32) {
		return [32]uint32{0: uint32(i)}, [32]uint32{0: uint32(i)}
	})
}
