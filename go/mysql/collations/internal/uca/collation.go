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

type WeightTable []*[]uint16

type Collation900 struct {
	table     WeightTable
	implicits func([]uint16, rune)
	contract  Contractor
	param     *parametricT
	maxLevel  int
	iterpool  *sync.Pool
}

func (c *Collation900) Weights() (WeightTable, TableLayout) {
	return c.table, TableLayout_uca900{}
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

func NewCollation(name string, weights WeightTable, weightPatches []WeightPatch, reorder []Reorder, contract Contractor, upperCaseFirst bool, levels int) *Collation900 {
	coll := &Collation900{
		table:     applyTailoring(TableLayout_uca900{}, weights, weightPatches),
		implicits: UnicodeImplicitWeights900,
		contract:  contract,
		maxLevel:  levels,
		param:     newParametricTailoring(reorder, upperCaseFirst),
		iterpool:  &sync.Pool{},
	}

	switch {
	case coll.param == nil && len(weightPatches) == 0 && coll.contract == nil:
		coll.iterpool.New = func() interface{} {
			return &FastIterator900{iterator900: iterator900{Collation900: *coll}}
		}
	case name == "utf8mb4_ja_0900_as_cs_ks" || name == "utf8mb4_ja_0900_as_cs":
		coll.iterpool.New = func() interface{} {
			return &jaIterator900{iterator900: iterator900{Collation900: *coll}}
		}
	case name == "utf8mb4_zh_0900_as_cs":
		coll.implicits = unicodeImplicitChineseWeights
		fallthrough
	default:
		coll.iterpool.New = func() interface{} {
			return &slowIterator900{iterator900: iterator900{Collation900: *coll}}
		}
	}

	return coll
}

type CollationLegacy struct {
	charset      charset.Charset
	table        WeightTable
	maxCodepoint rune
	contract     Contractor
	iterpool     *sync.Pool
}

func (c *CollationLegacy) Weights() (WeightTable, TableLayout) {
	return c.table, TableLayout_uca_legacy{c.maxCodepoint}
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

func NewCollationLegacy(cs charset.Charset, weights WeightTable, weightPatches []WeightPatch, contract Contractor, maxCodepoint rune) *CollationLegacy {
	coll := &CollationLegacy{
		charset:      cs,
		table:        applyTailoring(TableLayout_uca_legacy{}, weights, weightPatches),
		maxCodepoint: maxCodepoint,
		contract:     contract,
		iterpool:     &sync.Pool{},
	}

	coll.iterpool.New = func() interface{} {
		return &WeightIteratorLegacy{CollationLegacy: *coll}
	}

	return coll
}
