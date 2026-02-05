// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// TODO: remove hard-coded versions when we have implemented fractional weights.
// The current implementation is incompatible with later CLDR versions.
//go:generate go run maketables.go -cldr=23 -unicode=6.2.0

// Package collate contains types for comparing and sorting Unicode strings
// according to a given collation order.
package collate // import "vitess.io/vitess/go/mysql/collations/vindex/collate"

import (
	"hash"

	"vitess.io/vitess/go/mysql/collations/vindex/internal/colltab"
)

type Hasher struct {
	iter    colltab.Iter
	hash    hash.Hash
	scratch [32]colltab.Elem
}

// New returns a new Hasher initialized for the given hash function
func New(h hash.Hash) *Hasher {
	c := &Hasher{}
	c.iter.Weighter = getTable(tableIndex{0x15, 0x0})
	c.iter.Elems = c.scratch[:0]
	c.hash = h
	return c
}

func (c *Hasher) Hash(str []byte) []byte {
	c.hash.Reset()
	c.iter.SetInput(str)

	var scratch [64]byte
	var pos int

	for c.iter.Next() {
		for n := 0; n < c.iter.N; n++ {
			if w := c.iter.Elems[n].Primary(); w > 0 {
				if w <= 0x7FFF {
					if len(scratch)-pos < 2 {
						c.hash.Write(scratch[:pos])
						pos = 0
					}
					scratch[pos+0] = uint8(w >> 8)
					scratch[pos+1] = uint8(w)
					pos += 2
				} else {
					if len(scratch)-pos < 3 {
						c.hash.Write(scratch[:pos])
						pos = 0
					}
					scratch[pos+0] = uint8(w>>16) | 0x80
					scratch[pos+1] = uint8(w >> 8)
					scratch[pos+2] = uint8(w)
					pos += 3
				}
			}
		}
		c.iter.Discard()
	}
	c.hash.Write(scratch[:pos])
	return c.hash.Sum(nil)
}
