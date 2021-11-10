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
	"reflect"
	"sync"
	"unsafe"
)

const MaxCodepoint = 0x10FFFF + 1
const CodepointsPerPage = 256
const MaxCollationElementsPerCodepoint = 8

func PageOffset(cp rune) (int, int) {
	return int(cp) >> 8, int(cp) & 0xFF
}

type Weights []*[]uint16

type Patch struct {
	Codepoint rune
	Patch     []uint16
}

type Layout interface {
	MaxCodepoint() rune
	DebugWeights(table Weights, codepoint rune) []uint16

	allocPage(original *[]uint16, patches []Patch) []uint16
	applyPatches(page []uint16, offset int, weights []uint16)
}

type Layout_uca900 struct{}

func (Layout_uca900) MaxCodepoint() rune {
	return MaxCodepoint - 1
}

func (Layout_uca900) DebugWeights(table Weights, codepoint rune) (result []uint16) {
	p, offset := PageOffset(codepoint)
	page := table[p]
	if page == nil {
		return nil
	}

	ceCount := int((*page)[offset])
	for ce := 0; ce < ceCount; ce++ {
		result = append(result,
			(*page)[256+(ce*3+0)*256+offset],
			(*page)[256+(ce*3+1)*256+offset],
			(*page)[256+(ce*3+2)*256+offset],
		)
	}
	return
}

func (Layout_uca900) allocPage(original *[]uint16, patches []Patch) []uint16 {
	var maxWeights int
	for _, p := range patches {
		if len(p.Patch)%3 != 0 {
			panic("len(p.Patch)%3")
		}
		if len(p.Patch) > maxWeights {
			maxWeights = len(p.Patch)
		}
	}

	minLenForPage := maxWeights*CodepointsPerPage + CodepointsPerPage
	if original == nil {
		return make([]uint16, minLenForPage)
	}
	if len(*original) > minLenForPage {
		minLenForPage = len(*original)
	}
	newPage := make([]uint16, minLenForPage)
	copy(newPage, *original)
	return newPage
}

func (Layout_uca900) applyPatches(page []uint16, offset int, weights []uint16) {
	var weightcount = len(weights) / 3
	var ce int
	for ce < weightcount {
		page[(ce*3+1)*CodepointsPerPage+offset] = weights[ce*3+0]
		page[(ce*3+2)*CodepointsPerPage+offset] = weights[ce*3+1]
		page[(ce*3+3)*CodepointsPerPage+offset] = weights[ce*3+2]
		ce++
	}

	for ce < int(page[offset]) {
		page[(ce*3+1)*CodepointsPerPage+offset] = 0x0
		page[(ce*3+2)*CodepointsPerPage+offset] = 0x0
		page[(ce*3+3)*CodepointsPerPage+offset] = 0x0
		ce++
	}

	page[offset] = uint16(weightcount)
}

type Layout_uca_legacy struct {
	Max rune
}

func (l Layout_uca_legacy) MaxCodepoint() rune {
	return l.Max
}

func (l Layout_uca_legacy) DebugWeights(table Weights, codepoint rune) (result []uint16) {
	if codepoint > l.Max {
		return nil
	}
	p, offset := PageOffset(codepoint)
	page := table[p]
	if page == nil {
		return nil
	}

	stride := int((*page)[0])
	position := 1 + stride*offset
	weights := (*page)[position : position+stride]

	for i, w := range weights {
		if w == 0x0 {
			weights = weights[:i]
			break
		}
	}
	return weights
}

func (Layout_uca_legacy) allocPage(original *[]uint16, patches []Patch) []uint16 {
	var newStride int
	for _, p := range patches {
		if len(p.Patch) > newStride {
			newStride = len(p.Patch)
		}
	}

	minLenForPage := 1 + newStride*CodepointsPerPage
	if original == nil {
		return make([]uint16, minLenForPage)
	}

	if len(*original) >= minLenForPage {
		newPage := make([]uint16, len(*original))
		copy(newPage, *original)
		return newPage
	}

	originalStride := int((*original)[0])
	if originalStride >= newStride {
		panic("mismatch in originalStride calculation?")
	}

	newPage := make([]uint16, minLenForPage)
	for i := 0; i < CodepointsPerPage; i++ {
		for j := 0; j < originalStride; j++ {
			newPage[1+i*newStride] = (*original)[1+i*originalStride]
		}
	}
	return newPage
}

func (Layout_uca_legacy) applyPatches(page []uint16, offset int, weights []uint16) {
	stride := int(page[0])
	var ce int
	for ce < len(weights) {
		page[1+offset*stride+ce] = weights[ce]
		ce++
	}
	for ce < stride {
		page[1+offset*stride+ce] = 0x0
		ce++
	}
}

type tableWithPatch struct {
	tableptr uintptr
	patchptr uintptr
}

var cachedTables = make(map[tableWithPatch]Weights)
var cachedTablesMu sync.Mutex

func lookupCachedTable(table Weights, patch []Patch) (Weights, bool) {
	hdr1 := (*reflect.SliceHeader)(unsafe.Pointer(&table))
	hdr2 := (*reflect.SliceHeader)(unsafe.Pointer(&patch))

	cachedTablesMu.Lock()
	defer cachedTablesMu.Unlock()
	tbl, ok := cachedTables[tableWithPatch{hdr1.Data, hdr2.Data}]
	return tbl, ok
}

func storeCachedTable(table Weights, patch []Patch, result Weights) {
	hdr1 := (*reflect.SliceHeader)(unsafe.Pointer(&table))
	hdr2 := (*reflect.SliceHeader)(unsafe.Pointer(&patch))

	cachedTablesMu.Lock()
	cachedTables[tableWithPatch{hdr1.Data, hdr2.Data}] = result
	cachedTablesMu.Unlock()
}

func ApplyTailoring(layout Layout, base Weights, patches []Patch) Weights {
	if len(patches) == 0 {
		return base
	}
	if result, ok := lookupCachedTable(base, patches); ok {
		return result
	}

	result := make(Weights, len(base))
	copy(result, base)

	groups := make(map[int][]Patch)
	for _, patch := range patches {
		p, _ := PageOffset(patch.Codepoint)
		groups[p] = append(groups[p], patch)
	}

	for p, pps := range groups {
		page := layout.allocPage(result[p], pps)

		for _, patch := range pps {
			_, off := PageOffset(patch.Codepoint)
			layout.applyPatches(page, off, patch.Patch)
		}

		result[p] = &page
	}

	storeCachedTable(base, patches, result)
	return result
}
