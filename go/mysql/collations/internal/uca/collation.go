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

	"vitess.io/vitess/go/mysql/collations/internal/encoding"
)

type WeightTable []*[]uint16

type Collation900 struct {
	table        WeightTable
	implicits    func([]uint16, rune)
	contractions *contractions
	param        *parametricT
	maxLevel     int
	iterpool     *sync.Pool
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

func NewCollation(name string, weights WeightTable, weightPatches []WeightPatch, reorder []Reorder, contractions []Contraction, upperCaseFirst bool, levels int) *Collation900 {
	coll := &Collation900{
		table:        applyTailoring(TableLayout_uca900{}, weights, weightPatches),
		implicits:    UnicodeImplicitWeights900,
		maxLevel:     levels,
		param:        newParametricTailoring(reorder, upperCaseFirst),
		contractions: newContractions(contractions),
		iterpool:     &sync.Pool{},
	}

	switch {
	case coll.param == nil && coll.contractions == nil && len(weightPatches) == 0:
		coll.iterpool.New = func() interface{} {
			return &iteratorFast{iterator: iterator{Collation900: *coll}}
		}
	case name == "utf8mb4_ja_0900_as_cs_ks" || name == "utf8mb4_ja_0900_as_cs":
		coll.iterpool.New = func() interface{} {
			return &iteratorJA{iterator: iterator{Collation900: *coll}}
		}
	case name == "utf8mb4_zh_0900_as_cs":
		coll.implicits = unicodeImplicitChineseWeights
		fallthrough
	default:
		coll.iterpool.New = func() interface{} {
			return &iteratorSlow{iterator: iterator{Collation900: *coll}}
		}
	}

	return coll
}

type CollationLegacy struct {
	encoding     encoding.Encoding
	table        WeightTable
	maxCodepoint rune
	contractions *contractions
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

func NewCollationLegacy(enc encoding.Encoding, weights WeightTable, weightPatches []WeightPatch, contractions []Contraction, maxCodepoint rune) *CollationLegacy {
	coll := &CollationLegacy{
		encoding:     enc,
		table:        applyTailoring(TableLayout_uca_legacy{}, weights, weightPatches),
		maxCodepoint: maxCodepoint,
		contractions: newContractions(contractions),
		iterpool:     &sync.Pool{},
	}

	coll.iterpool.New = func() interface{} {
		return &WeightIteratorLegacy{CollationLegacy: *coll}
	}

	return coll
}
