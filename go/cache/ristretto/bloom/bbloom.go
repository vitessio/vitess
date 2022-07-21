// The MIT License (MIT)
// Copyright (c) 2014 Andreas Briese, eduToolbox@Bri-C GmbH, Sarstedt

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package bloom

import (
	"math"
	"unsafe"
)

// helper
var mask = []uint8{1, 2, 4, 8, 16, 32, 64, 128}

func getSize(ui64 uint64) (size uint64, exponent uint64) {
	if ui64 < uint64(512) {
		ui64 = uint64(512)
	}
	size = uint64(1)
	for size < ui64 {
		size <<= 1
		exponent++
	}
	return size, exponent
}

// NewBloomFilterWithErrorRate returns a new bloomfilter with optimal size for the given
// error rate
func NewBloomFilterWithErrorRate(numEntries uint64, wrongs float64) *Bloom {
	size := -1 * float64(numEntries) * math.Log(wrongs) / math.Pow(0.69314718056, 2)
	locs := math.Ceil(0.69314718056 * size / float64(numEntries))
	return NewBloomFilter(uint64(size), uint64(locs))
}

// NewBloomFilter returns a new bloomfilter.
func NewBloomFilter(entries, locs uint64) (bloomfilter *Bloom) {
	size, exponent := getSize(entries)
	bloomfilter = &Bloom{
		sizeExp: exponent,
		size:    size - 1,
		setLocs: locs,
		shift:   64 - exponent,
	}
	bloomfilter.Size(size)
	return bloomfilter
}

// Bloom filter
type Bloom struct {
	bitset  []uint64
	ElemNum uint64
	sizeExp uint64
	size    uint64
	setLocs uint64
	shift   uint64
}

// <--- http://www.cse.yorku.ca/~oz/hash.html
// modified Berkeley DB Hash (32bit)
// hash is casted to l, h = 16bit fragments
// func (bl Bloom) absdbm(b *[]byte) (l, h uint64) {
// 	hash := uint64(len(*b))
// 	for _, c := range *b {
// 		hash = uint64(c) + (hash << 6) + (hash << bl.sizeExp) - hash
// 	}
// 	h = hash >> bl.shift
// 	l = hash << bl.shift >> bl.shift
// 	return l, h
// }

// Add adds hash of a key to the bloomfilter.
func (bl *Bloom) Add(hash uint64) {
	h := hash >> bl.shift
	l := hash << bl.shift >> bl.shift
	for i := uint64(0); i < bl.setLocs; i++ {
		bl.Set((h + i*l) & bl.size)
		bl.ElemNum++
	}
}

// Has checks if bit(s) for entry hash is/are set,
// returns true if the hash was added to the Bloom Filter.
func (bl Bloom) Has(hash uint64) bool {
	h := hash >> bl.shift
	l := hash << bl.shift >> bl.shift
	for i := uint64(0); i < bl.setLocs; i++ {
		if !bl.IsSet((h + i*l) & bl.size) {
			return false
		}
	}
	return true
}

// AddIfNotHas only Adds hash, if it's not present in the bloomfilter.
// Returns true if hash was added.
// Returns false if hash was already registered in the bloomfilter.
func (bl *Bloom) AddIfNotHas(hash uint64) bool {
	if bl.Has(hash) {
		return false
	}
	bl.Add(hash)
	return true
}

// TotalSize returns the total size of the bloom filter.
func (bl *Bloom) TotalSize() int {
	// The bl struct has 5 members and each one is 8 byte. The bitset is a
	// uint64 byte slice.
	return len(bl.bitset)*8 + 5*8
}

// Size makes Bloom filter with as bitset of size sz.
func (bl *Bloom) Size(sz uint64) {
	bl.bitset = make([]uint64, sz>>6)
}

// Clear resets the Bloom filter.
func (bl *Bloom) Clear() {
	for i := range bl.bitset {
		bl.bitset[i] = 0
	}
}

// Set sets the bit[idx] of bitset.
func (bl *Bloom) Set(idx uint64) {
	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(&bl.bitset[idx>>6])) + uintptr((idx%64)>>3))
	*(*uint8)(ptr) |= mask[idx%8]
}

// IsSet checks if bit[idx] of bitset is set, returns true/false.
func (bl *Bloom) IsSet(idx uint64) bool {
	ptr := unsafe.Pointer(uintptr(unsafe.Pointer(&bl.bitset[idx>>6])) + uintptr((idx%64)>>3))
	r := ((*(*uint8)(ptr)) >> (idx % 8)) & 1
	return r == 1
}
