// Chunk test code is modified from slices/iter_test.go
//
// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package vstreamer

import (
	"reflect"
	"slices"
	"testing"
)

func TestChunk(t *testing.T) {
	cases := []struct {
		name   string
		s      []int
		n      int
		chunks [][]int
	}{
		{
			name:   "nil",
			s:      nil,
			n:      1,
			chunks: nil,
		},
		{
			name:   "empty",
			s:      []int{},
			n:      1,
			chunks: nil,
		},
		{
			name:   "short",
			s:      []int{1, 2},
			n:      3,
			chunks: [][]int{{1, 2}},
		},
		{
			name:   "one",
			s:      []int{1, 2},
			n:      2,
			chunks: [][]int{{1, 2}},
		},
		{
			name:   "even",
			s:      []int{1, 2, 3, 4},
			n:      2,
			chunks: [][]int{{1, 2}, {3, 4}},
		},
		{
			name:   "odd",
			s:      []int{1, 2, 3, 4, 5},
			n:      2,
			chunks: [][]int{{1, 2}, {3, 4}, {5}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var chunks [][]int
			for c := range reflectChunk(reflect.ValueOf(tc.s), tc.n) {
				typedC := c.Interface().([]int)
				chunks = append(chunks, typedC)
			}

			if !chunkEqual(chunks, tc.chunks) {
				t.Errorf("Chunk(%v, %d) = %v, want %v", tc.s, tc.n, chunks, tc.chunks)
			}

			if len(chunks) == 0 {
				return
			}

			// Verify that appending to the end of the first chunk does not
			// clobber the beginning of the next chunk.
			s := slices.Clone(tc.s)
			chunks[0] = append(chunks[0], -1)
			if !slices.Equal(s, tc.s) {
				t.Errorf("slice was clobbered: %v, want %v", s, tc.s)
			}
		})
	}
}

func TestChunkPanics(t *testing.T) {
	for _, test := range []struct {
		name string
		x    []struct{}
		n    int
	}{
		{
			name: "cannot be less than 1",
			x:    make([]struct{}, 0),
			n:    0,
		},
	} {
		if !panics(func() { _ = reflectChunk(reflect.ValueOf(test.x), test.n) }) {
			t.Errorf("Chunk %s: got no panic, want panic", test.name)
		}
	}
}

func TestChunkRange(t *testing.T) {
	// Verify Chunk iteration can be stopped.
	var got [][]int
	for c := range reflectChunk(reflect.ValueOf([]int{1, 2, 3, 4, -100}), 2) {
		if len(got) == 2 {
			// Found enough values, break early.
			break
		}

		typedC := c.Interface().([]int)
		got = append(got, typedC)
	}

	if want := [][]int{{1, 2}, {3, 4}}; !chunkEqual(got, want) {
		t.Errorf("Chunk iteration did not stop, got %v, want %v", got, want)
	}
}

func chunkEqual[Slice ~[]E, E comparable](s1, s2 []Slice) bool {
	return slices.EqualFunc(s1, s2, slices.Equal[Slice])
}

func panics(f func()) (b bool) {
	defer func() {
		if x := recover(); x != nil {
			b = true
		}
	}()
	f()
	return false
}
