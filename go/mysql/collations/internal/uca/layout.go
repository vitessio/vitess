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

func equalWeights900(table Weights, levels int, A, B rune) bool {
	pA, offsetA := PageOffset(A)
	pageA := table[pA]

	pB, offsetB := PageOffset(B)
	pageB := table[pB]

	if pageA == nil || pageB == nil {
		return false
	}

	ppA := (*pageA)[256+offsetA:]
	ppB := (*pageB)[256+offsetB:]

	if ppA[0] != 0x0 && ppB[0] != 0x0 && ppA[0] != ppB[0] {
		return false
	}

	cA := int((*pageA)[offsetA])
	cB := int((*pageB)[offsetB])

	for l := 0; l < levels; l++ {
		wA, wB := l*256, l*256
		wA1, wB1 := wA+(cA*256*3), wB+(cB*256*3)

		for wA < wA1 && wB < wB1 {
			for wA < wA1 && ppA[wA] == 0x0 {
				wA += 256 * 3
			}
			if wA == wA1 {
				break
			}
			for wB < wB1 && ppB[wB] == 0x0 {
				wB += 256 * 3
			}
			if wB == wB1 {
				break
			}
			if ppA[wA] != ppB[wB] {
				return false
			}
			wA += 256 * 3
			wB += 256 * 3
		}
		for wA < wA1 {
			if ppA[wA] != 0x0 {
				return false
			}
			wA += 256 * 3
		}
		for wB < wB1 {
			if ppB[wB] != 0x0 {
				return false
			}
			wB += 256 * 3
		}
	}
	return true
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

func equalWeightsLegacy(table Weights, A, B rune) bool {
	pA, offsetA := PageOffset(A)
	pageA := table[pA]

	pB, offsetB := PageOffset(B)
	pageB := table[pB]

	if pageA == nil || pageB == nil {
		return false
	}

	sA := int((*pageA)[0])
	sB := int((*pageB)[0])
	iA := 1 + sA*offsetA
	iB := 1 + sB*offsetB

	var shrt, long []uint16
	if sA < sB {
		shrt = (*pageA)[iA : iA+sA]
		long = (*pageB)[iB : iB+sB]
	} else {
		shrt = (*pageB)[iB : iB+sB]
		long = (*pageA)[iA : iA+sA]
	}

	for i, wA := range shrt {
		wB := long[i]
		if wA != wB {
			return false
		}
	}
	if len(long) > len(shrt) && long[len(shrt)] != 0x0 {
		return false
	}
	return true
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
	tableptr unsafe.Pointer
	patchptr unsafe.Pointer
}

var cachedTables = make(map[tableWithPatch]Weights)
var cachedTablesMu sync.Mutex

func lookupCachedTable(table Weights, patch []Patch) (Weights, bool) {
	data1 := unsafe.Pointer(unsafe.SliceData(table))
	data2 := unsafe.Pointer(unsafe.SliceData(patch))

	cachedTablesMu.Lock()
	defer cachedTablesMu.Unlock()
	tbl, ok := cachedTables[tableWithPatch{tableptr: data1, patchptr: data2}]
	return tbl, ok
}

func storeCachedTable(table Weights, patch []Patch, result Weights) {
	data1 := unsafe.Pointer(unsafe.SliceData(table))
	data2 := unsafe.Pointer(unsafe.SliceData(patch))

	cachedTablesMu.Lock()
	cachedTables[tableWithPatch{tableptr: data1, patchptr: data2}] = result
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
