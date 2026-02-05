/*
Copyright 2023 The Vitess Authors.
Copyright 2023 Yiling-J

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

package theine

type CountMinSketch struct {
	Table      []uint64
	Additions  uint
	SampleSize uint
	BlockMask  uint
}

func NewCountMinSketch() *CountMinSketch {
	new := &CountMinSketch{}
	new.EnsureCapacity(16)
	return new
}

// indexOf return table index and counter index together
func (s *CountMinSketch) indexOf(h uint64, block uint64, offset uint8) (uint, uint) {
	counterHash := h + uint64(1+offset)*(h>>32)
	// max block + 7(8 * 8 bytes), fit 64 bytes cache line
	index := block + counterHash&1 + uint64(offset<<1)
	return uint(index), uint((counterHash & 0xF) << 2)
}

func (s *CountMinSketch) inc(index uint, offset uint) bool {
	mask := uint64(0xF << offset)
	if s.Table[index]&mask != mask {
		s.Table[index] += 1 << offset
		return true
	}
	return false
}

func (s *CountMinSketch) Add(h uint64) bool {
	hn := spread(h)
	block := (hn & uint64(s.BlockMask)) << 3
	hc := rehash(h)
	index0, offset0 := s.indexOf(hc, block, 0)
	index1, offset1 := s.indexOf(hc, block, 1)
	index2, offset2 := s.indexOf(hc, block, 2)
	index3, offset3 := s.indexOf(hc, block, 3)

	added := s.inc(index0, offset0)
	added = s.inc(index1, offset1) || added
	added = s.inc(index2, offset2) || added
	added = s.inc(index3, offset3) || added

	if added {
		s.Additions += 1
		if s.Additions == s.SampleSize {
			s.reset()
			return true
		}
	}
	return false
}

func (s *CountMinSketch) reset() {
	for i := range s.Table {
		s.Table[i] = s.Table[i] >> 1
	}
	s.Additions = s.Additions >> 1
}

func (s *CountMinSketch) count(h uint64, block uint64, offset uint8) uint {
	index, off := s.indexOf(h, block, offset)
	count := (s.Table[index] >> off) & 0xF
	return uint(count)
}

func (s *CountMinSketch) Estimate(h uint64) uint {
	hn := spread(h)
	block := (hn & uint64(s.BlockMask)) << 3
	hc := rehash(h)
	m := min(s.count(hc, block, 0), 100)
	m = min(s.count(hc, block, 1), m)
	m = min(s.count(hc, block, 2), m)
	m = min(s.count(hc, block, 3), m)
	return m
}

func next2Power(x uint) uint {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

func (s *CountMinSketch) EnsureCapacity(size uint) {
	if len(s.Table) >= int(size) {
		return
	}
	if size < 16 {
		size = 16
	}
	newSize := next2Power(size)
	s.Table = make([]uint64, newSize)
	s.SampleSize = 10 * size
	s.BlockMask = uint((len(s.Table) >> 3) - 1)
	s.Additions = 0
}

func spread(h uint64) uint64 {
	h ^= h >> 17
	h *= 0xed5ad4bb
	h ^= h >> 11
	h *= 0xac4c1b51
	h ^= h >> 15
	return h
}

func rehash(h uint64) uint64 {
	h *= 0x31848bab
	h ^= h >> 14
	return h
}
