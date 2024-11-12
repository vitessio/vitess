package vstreamer

import (
	"iter"
	"reflect"
)

// Chunk returns an iterator over consecutive sub-slices of up to n elements of s.
// All but the last sub-slice will have size n.
// All sub-slices are clipped to have no capacity beyond the length.
// If s is empty, the sequence is empty: there is no empty slice in the sequence.
// Chunk panics if n is less than 1.
func reflectChunk(s reflect.Value, n int) iter.Seq[reflect.Value] {
	if n < 1 {
		panic("cannot be less than 1")
	}

	if s.Kind() != reflect.Slice {
		panic("must be a slice")
	}

	return func(yield func(s reflect.Value) bool) {
		for i := 0; i < s.Len(); i += n {
			// Clamp the last chunk to the slice bound as necessary.
			// end := min(n, len(s[i:]))
			end := min(n, s.Slice(i, s.Len()).Len())

			// Set the capacity of each chunk so that appending to a chunk does
			// not modify the original slice.
			if !yield(s.Slice3(i, i+end, i+end)) {
				return
			}
		}
	}
}
