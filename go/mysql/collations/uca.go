/*
Copyright 2021 The Vitess Authors.

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

package collations

import (
	"sync"

	"vitess.io/vitess/go/mysql/collations/internal/encoding"
	"vitess.io/vitess/go/mysql/collations/internal/uca"
)

func init() {
	register(&Collation_utf8mb4_0900_bin{})
}

type CollationUCA interface {
	Collation
	Encoding() encoding.Encoding
	UnicodeWeightsTable() (uca.WeightTable, uca.TableLayout)
}

type Collation_utf8mb4_uca_0900 struct {
	name string
	id   ID

	weights          uca.WeightTable
	tailoring        []uca.WeightPatch
	contractions     []uca.Contraction
	reorder          []uca.Reorder
	upperCaseFirst   bool
	levelsForCompare int

	uca     *uca.Collation900
	ucainit sync.Once
}

func (c *Collation_utf8mb4_uca_0900) init() {
	c.ucainit.Do(func() {
		c.uca = uca.NewCollation(c.name, c.weights, c.tailoring, c.reorder, c.contractions, c.upperCaseFirst, c.levelsForCompare)

		// Clear the external metadata for this collation, so it can be picked up by the GC
		c.weights = nil
		c.contractions = nil
		c.tailoring = nil
		c.reorder = nil
	})
}

func (c *Collation_utf8mb4_uca_0900) Encoding() encoding.Encoding {
	return encoding.Encoding_utf8mb4{}
}

func (c *Collation_utf8mb4_uca_0900) UnicodeWeightsTable() (uca.WeightTable, uca.TableLayout) {
	return c.uca.Weights()
}

func (c *Collation_utf8mb4_uca_0900) Name() string {
	return c.name
}

func (c *Collation_utf8mb4_uca_0900) Id() ID {
	return c.id
}

func (c *Collation_utf8mb4_uca_0900) Collate(left, right []byte, rightIsPrefix bool) int {
	var (
		l, r            uint16
		lok, rok        bool
		level           int
		levelsToCompare = c.levelsForCompare
		itleft          = c.uca.Iterator(left)
		itright         = c.uca.Iterator(right)
	)

	defer itleft.Done()
	defer itright.Done()

nextLevel:
	for {
		l, lok = itleft.Next()
		r, rok = itright.Next()

		if l != r || !lok || !rok {
			break
		}
		if itleft.Level() != level || itright.Level() != level {
			break
		}
	}

	switch {
	case itleft.Level() == itright.Level():
		if l == r && lok && rok {
			level = itleft.Level()
			if level < levelsToCompare {
				goto nextLevel
			}
		}
	case itleft.Level() > level:
		return -1
	case itright.Level() > level:
		// TODO@vmg: this is not fully correct
		if rightIsPrefix {
			if itleft.SkipLevel() {
				level = itleft.Level()
				if level < levelsToCompare {
					goto nextLevel
				}
			}
			break
		}
		return 1
	}

	return int(l) - int(r)
}

func (c *Collation_utf8mb4_uca_0900) WeightString(dst, src []byte, numCodepoints int) []byte {
	it := c.uca.Iterator(src)
	defer it.Done()

	for {
		w, ok := it.Next()
		if !ok {
			break
		}
		dst = append(dst, byte(w>>8), byte(w))
	}

	if numCodepoints == PadToMax {
		for len(dst) < cap(dst) {
			dst = append(dst, 0x00)
		}
	}

	return dst
}

func (c *Collation_utf8mb4_uca_0900) WeightStringLen(numBytes int) int {
	if numBytes%4 != 0 {
		panic("WeightStringLen called with non-MOD4 length")
	}
	levels := int(c.levelsForCompare)
	weights := (numBytes / 4) * uca.MaxCollationElementsPerCodepoint * levels
	weights += levels - 1 // one NULL byte as a separator between levels
	return weights * 2    // two bytes per weight
}

type Collation_utf8mb4_0900_bin struct{}

func (c *Collation_utf8mb4_0900_bin) init() {}

func (c *Collation_utf8mb4_0900_bin) Encoding() encoding.Encoding {
	return encoding.Encoding_utf8mb4{}
}

func (c *Collation_utf8mb4_0900_bin) Id() ID {
	return 309
}

func (c *Collation_utf8mb4_0900_bin) Name() string {
	return "utf8mb4_0900_bin"
}

func (c *Collation_utf8mb4_0900_bin) Collate(left, right []byte, isPrefix bool) int {
	return collationBinary(left, right, isPrefix)
}

func (c *Collation_utf8mb4_0900_bin) WeightString(dst, src []byte, numCodepoints int) []byte {
	copyCodepoints := minInt(len(src), cap(dst))
	dst = append(dst, src[:copyCodepoints]...)
	if numCodepoints == PadToMax {
		for len(dst) < cap(dst) {
			dst = append(dst, 0x0)
		}
	}
	return dst
}

func (c *Collation_utf8mb4_0900_bin) WeightStringLen(numBytes int) int {
	return numBytes
}

type Collation_uca_legacy struct {
	name string
	id   ID

	charset      encoding.Encoding
	weights      uca.WeightTable
	tailoring    []uca.WeightPatch
	contractions []uca.Contraction
	maxCodepoint rune

	uca     *uca.CollationLegacy
	ucainit sync.Once
}

func (c *Collation_uca_legacy) init() {
	c.ucainit.Do(func() {
		c.uca = uca.NewCollationLegacy(c.charset, c.weights, c.tailoring, c.contractions, c.maxCodepoint)
		c.weights = nil
		c.tailoring = nil
		c.contractions = nil
	})
}

func (c *Collation_uca_legacy) Encoding() encoding.Encoding {
	return c.charset
}

func (c *Collation_uca_legacy) UnicodeWeightsTable() (uca.WeightTable, uca.TableLayout) {
	return c.uca.Weights()
}

func (c *Collation_uca_legacy) Id() ID {
	return c.id
}

func (c *Collation_uca_legacy) Name() string {
	return c.name
}

func (c *Collation_uca_legacy) Collate(left, right []byte, isPrefix bool) int {
	var (
		l, r     uint16
		lok, rok bool
		itleft   = c.uca.Iterator(left)
		itright  = c.uca.Iterator(right)
	)

	defer itleft.Done()
	defer itright.Done()

	if isPrefix {
		panic("unimplemented: isPrefix")
	}

	for {
		l, lok = itleft.Next()
		r, rok = itright.Next()

		if l == r && lok && rok {
			continue
		}

		return int(l) - int(r)
	}
}

func (c *Collation_uca_legacy) WeightString(dst, src []byte, numCodepoints int) []byte {
	it := c.uca.Iterator(src)
	defer it.Done()

	for {
		w, ok := it.Next()
		if !ok {
			break
		}
		dst = append(dst, byte(w>>8), byte(w))
	}

	if numCodepoints > 0 {
		weightForSpace := c.uca.WeightForSpace()
		w1, w2 := byte(weightForSpace>>8), byte(weightForSpace)

		if numCodepoints == PadToMax {
			for len(dst)+1 < cap(dst) {
				dst = append(dst, w1, w2)
			}
			if len(dst) < cap(dst) {
				dst = append(dst, w1)
			}
		} else {
			numCodepoints -= it.Length()
			for numCodepoints > 0 {
				dst = append(dst, w1, w2)
				numCodepoints--
			}
		}
	}

	return dst
}

func (c *Collation_uca_legacy) WeightStringLen(numBytes int) int {
	// TODO: This is literally the worst case scenario. Improve on this.
	return numBytes * 8
}
