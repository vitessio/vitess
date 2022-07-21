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

package uca

import (
	"sync"

	"vitess.io/vitess/go/mysql/collations/internal/charset"
)

type Collation interface {
	Charset() charset.Charset
	Weights() (Weights, Layout)
	WeightForSpace() uint16
	WeightsEqual(left, right rune) bool
}

var _ Collation = (*Collation900)(nil)

type Collation900 struct {
	table     Weights
	implicits func([]uint16, rune)
	contract  Contractor
	param     *parametricT
	maxLevel  int
	iterpool  *sync.Pool
}

func (c *Collation900) Charset() charset.Charset {
	return charset.Charset_utf8mb4{}
}

func (c *Collation900) Weights() (Weights, Layout) {
	return c.table, Layout_uca900{}
}

func (c *Collation900) Iterator(input []byte) WeightIterator {
	iter := c.iterpool.Get().(WeightIterator)
	iter.reset(input)
	return iter
}

func (c *Collation900) WeightForSpace() uint16 {
	ascii := *c.table[0]
	return ascii[CodepointsPerPage+' ']
}

func (c *Collation900) WeightsEqual(left, right rune) bool {
	if left == right {
		return true
	}
	return equalWeights900(c.table, c.maxLevel, left, right)
}

func NewCollation(name string, weights Weights, weightPatches []Patch, reorder []Reorder, contract Contractor, upperCaseFirst bool, levels int) *Collation900 {
	coll := &Collation900{
		table:     ApplyTailoring(Layout_uca900{}, weights, weightPatches),
		implicits: UnicodeImplicitWeights900,
		contract:  contract,
		maxLevel:  levels,
		param:     newParametricTailoring(reorder, upperCaseFirst),
		iterpool:  &sync.Pool{},
	}

	switch {
	case coll.param == nil && len(weightPatches) == 0 && coll.contract == nil:
		coll.iterpool.New = func() any {
			return &FastIterator900{iterator900: iterator900{Collation900: *coll}}
		}
	case name == "utf8mb4_ja_0900_as_cs_ks" || name == "utf8mb4_ja_0900_as_cs":
		coll.iterpool.New = func() any {
			return &jaIterator900{iterator900: iterator900{Collation900: *coll}}
		}
	case name == "utf8mb4_zh_0900_as_cs":
		coll.implicits = unicodeImplicitChineseWeights
		fallthrough
	default:
		coll.iterpool.New = func() any {
			return &slowIterator900{iterator900: iterator900{Collation900: *coll}}
		}
	}

	return coll
}

var _ Collation = (*CollationLegacy)(nil)

type CollationLegacy struct {
	charset      charset.Charset
	table        Weights
	maxCodepoint rune
	contract     Contractor
	iterpool     *sync.Pool
}

func (c *CollationLegacy) Charset() charset.Charset {
	return c.charset
}

func (c *CollationLegacy) Weights() (Weights, Layout) {
	return c.table, Layout_uca_legacy{Max: c.maxCodepoint}
}

func (c *CollationLegacy) Iterator(input []byte) *WeightIteratorLegacy {
	iter := c.iterpool.Get().(*WeightIteratorLegacy)
	iter.reset(input)
	return iter
}

func (c *CollationLegacy) WeightForSpace() uint16 {
	ascii := *c.table[0]
	stride := ascii[0]
	return ascii[1+' '*stride]
}

func (c *CollationLegacy) WeightsEqual(left, right rune) bool {
	if left == right {
		return true
	}
	return equalWeightsLegacy(c.table, left, right)
}

func NewCollationLegacy(cs charset.Charset, weights Weights, weightPatches []Patch, contract Contractor, maxCodepoint rune) *CollationLegacy {
	coll := &CollationLegacy{
		charset:      cs,
		table:        ApplyTailoring(Layout_uca_legacy{}, weights, weightPatches),
		maxCodepoint: maxCodepoint,
		contract:     contract,
		iterpool:     &sync.Pool{},
	}

	coll.iterpool.New = func() any {
		return &WeightIteratorLegacy{CollationLegacy: *coll}
	}

	return coll
}
