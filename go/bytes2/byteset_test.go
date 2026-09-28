/*
Copyright 2026 The Vitess Authors.

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

package bytes2

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// sqlEscapeBytes is the set the SQL literal encoders scan for, which is the
// production caller of ByteSet. It includes 0x00, the byte a zero-filled
// partial vector load would forge a hit for; the kernel reads its tail as an
// overlapping full block instead, and the tail tests below hold it to that.
var sqlEscapeBytes = []byte{0, '\'', '\b', '\n', '\r', '\t', 26, '\\'}

// refIndex is the naive reference both Index implementations are checked
// against: a byte loop over the set, independent of the membership table.
func refIndex(b []byte, set []byte) int {
	for i, c := range b {
		if slices.Contains(set, c) {
			return i
		}
	}
	return -1
}

// boundarySizes are the input lengths around the 128-, 256- and 512-bit
// vector widths, where the full-block loop hands over to the partial tail,
// and around IndexAny2's first two window edges (256 and 256+1024). 20, 25
// and 40 are clean lengths that are a multiple of no vector width: 0x00 is in
// the set the escapers scan for, so a zero-filled partial tail load would
// forge a hit at one of them.
var boundarySizes = []int{0, 1, 7, 8, 15, 16, 17, 20, 25, 31, 32, 33, 40, 63, 64, 65, 100, 128, 129, 255, 256, 257, 1279, 1280, 1281}

func clean(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(i%26)
	}
	return b
}

func TestNewByteSet(t *testing.T) {
	assert.Panics(t, func() { NewByteSet() })
	assert.Panics(t, func() { NewByteSet(1, 2, 3, 4, 5, 6, 7, 8, 9) })

	// Every pre-broadcast row is its member repeated across the widest
	// vector. Only the SIMD Index reads the rows, and that build is not the
	// default one, so this pins the fill in every build.
	rowsMatchMembers := func(t *testing.T, s *ByteSet, vals []byte) {
		t.Helper()
		for i := range s.bcast {
			v := vals[0]
			if i < len(vals) {
				v = vals[i]
			}
			assert.Equal(t, bytes.Repeat([]byte{v}, bcastWidth), s.bcast[i][:], "row %d", i)
		}
	}

	s := NewByteSet('x')
	assert.True(t, s.table['x'])
	assert.False(t, s.table['y'])
	rowsMatchMembers(t, s, []byte{'x'})

	s = NewByteSet(sqlEscapeBytes...)
	for _, v := range sqlEscapeBytes {
		assert.True(t, s.table[v], "byte %#x", v)
	}
	rowsMatchMembers(t, s, sqlEscapeBytes)
}

func TestByteSetIndex(t *testing.T) {
	set := NewByteSet(sqlEscapeBytes...)

	check := func(t *testing.T, name string, in []byte) {
		want := refIndex(in, sqlEscapeBytes)
		assert.Equal(t, want, set.indexScalar(in), "%s: scalar", name)
		assert.Equal(t, want, set.Index(in), "%s: Index", name)
	}

	for _, n := range boundarySizes {
		t.Run(fmt.Sprintf("size=%d", n), func(t *testing.T) {
			check(t, "clean", clean(n))
			// A hit at every position, including the last lane of a full
			// block and the first lane of the tail.
			for pos := range n {
				for _, v := range sqlEscapeBytes {
					in := clean(n)
					in[pos] = v
					check(t, fmt.Sprintf("byte %#x at %d", v, pos), in)
				}
			}
			// Two hits: the first one must win.
			if n >= 2 {
				in := clean(n)
				in[n-1] = '\\'
				in[n/2] = '\''
				check(t, "first of two", in)
			}
		})
	}

	// A set that is all one byte, which exercises the padding path. The
	// member is outside the a..z corpus clean produces.
	t.Run("single member", func(t *testing.T) {
		one := NewByteSet('#')
		in := clean(70)
		assert.Equal(t, -1, one.Index(in))
		in[69] = '#'
		assert.Equal(t, 69, one.Index(in))
		in[16] = '#'
		assert.Equal(t, 16, one.Index(in))
	})
}

func TestIndexAny2(t *testing.T) {
	check := func(t *testing.T, name string, in []byte, a, c byte) {
		assert.Equal(t, refIndex(in, []byte{a, c}), IndexAny2(string(in), a, c), name)
	}

	for _, n := range boundarySizes {
		t.Run(fmt.Sprintf("size=%d", n), func(t *testing.T) {
			check(t, "clean", clean(n), '\'', '\\')
			for pos := range n {
				in := clean(n)
				in[pos] = '\''
				check(t, fmt.Sprintf("quote at %d", pos), in, '\'', '\\')
				in = clean(n)
				in[pos] = '\\'
				check(t, fmt.Sprintf("backslash at %d", pos), in, '\'', '\\')
			}
			if n >= 2 {
				// The scalar path finds a first, then must prefer the earlier c.
				in := clean(n)
				in[n-1] = '\''
				in[0] = '\\'
				check(t, "c before a", in, '\'', '\\')
				// And the reverse.
				in = clean(n)
				in[0] = '\''
				in[n-1] = '\\'
				check(t, "a before c", in, '\'', '\\')
			}
		})
	}

	t.Run("zero as a needle", func(t *testing.T) {
		for _, n := range []int{17, 25, 33, 63} {
			in := clean(n)
			assert.Equal(t, -1, IndexAny2(string(in), 0, '"'), "size %d", n)
			in[n-1] = 0
			assert.Equal(t, n-1, IndexAny2(string(in), 0, '"'), "size %d", n)
		}
	})

	t.Run("same byte twice", func(t *testing.T) {
		in := clean(40)
		in[33] = '\''
		assert.Equal(t, 33, IndexAny2(string(in), '\'', '\''))
	})

	// The windows: a hit in a later window is found, the earlier of two hits
	// in different windows wins, and a hit for a in a later window does not
	// hide a hit for c in an earlier one.
	t.Run("windows", func(t *testing.T) {
		for _, pos := range []int{0, 255, 256, 257, 1279, 1280, 1281, 5000} {
			in := clean(6000)
			in[pos] = '\\'
			assert.Equal(t, pos, IndexAny2(string(in), '\'', '\\'), "lone c at %d", pos)
			in = clean(6000)
			in[pos] = '\''
			in[5999] = '\\'
			assert.Equal(t, pos, IndexAny2(string(in), '\'', '\\'), "a at %d before c at the end", pos)
			in = clean(6000)
			in[pos] = '\\'
			in[5999] = '\''
			assert.Equal(t, pos, IndexAny2(string(in), '\'', '\\'), "c at %d before a at the end", pos)
		}
	})
}

func FuzzByteSetIndex(f *testing.F) {
	set := NewByteSet(sqlEscapeBytes...)
	f.Add([]byte("hello"))
	f.Add(clean(64))
	f.Add(append(clean(31), 0))
	f.Add(append(clean(31), '\\'))
	f.Fuzz(func(t *testing.T, in []byte) {
		want := refIndex(in, sqlEscapeBytes)
		if got := set.Index(in); got != want {
			t.Fatalf("Index(%q) = %d, want %d", in, got, want)
		}
		if got := set.indexScalar(in); got != want {
			t.Fatalf("indexScalar(%q) = %d, want %d", in, got, want)
		}
	})
}

func FuzzIndexAny2(f *testing.F) {
	f.Add([]byte("hello"), byte('\''), byte('\\'))
	f.Add(clean(64), byte('"'), byte('\\'))
	f.Add(append(clean(31), 0), byte(0), byte('x'))
	f.Fuzz(func(t *testing.T, in []byte, a, c byte) {
		want := refIndex(in, []byte{a, c})
		if got := IndexAny2(string(in), a, c); got != want {
			t.Fatalf("IndexAny2(%q, %#x, %#x) = %d, want %d", in, a, c, got, want)
		}
	})
}

// benchInput mirrors the shapes the SQL encoders see: clean ASCII text, text
// with a rare special byte, and dense random binary.
func benchInput(size int, shape string) []byte {
	buf := clean(size)
	switch shape {
	case "clean":
	case "sparse":
		for i := min(size/2, 128); i < size; i += 256 {
			buf[i] = '\''
		}
	case "dense":
		rng := rand.New(rand.NewPCG(0x5eed, uint64(size)))
		for i := range buf {
			buf[i] = byte(rng.Uint32())
		}
	}
	return buf
}

// BenchmarkByteSetIndex scans the whole input the way the escaping loop
// does: from each hit to the next, until the end.
func BenchmarkByteSetIndex(b *testing.B) {
	set := NewByteSet(sqlEscapeBytes...)
	for _, size := range []int{8, 32, 256, 4096} {
		for _, shape := range []string{"clean", "sparse", "dense"} {
			in := benchInput(size, shape)
			b.Run(fmt.Sprintf("%d/%s", size, shape), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					for p := in; len(p) > 0; {
						i := set.Index(p)
						if i < 0 {
							break
						}
						p = p[i+1:]
					}
				}
			})
		}
	}
}

func BenchmarkIndexAny2(b *testing.B) {
	for _, size := range []int{16, 64, 256, 4096} {
		// Converted outside the loop: the tokenizer passes a substring of a
		// string it already has, so an allocation per call is not the shape
		// being measured.
		in := string(clean(size))
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if IndexAny2(in, '\'', '\\') != -1 {
					b.Fatal("unexpected hit")
				}
			}
		})
	}
}
