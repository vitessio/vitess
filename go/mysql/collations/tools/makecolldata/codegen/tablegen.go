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

package codegen

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"math/bits"
	"os"
	"reflect"

	"vitess.io/vitess/go/mysql/collations/internal/uca"
)

type LiteralPageGenerator struct {
	index map[string]string
}

func (pg *LiteralPageGenerator) WritePage16(g *Generator, varname string, values []uint16) string {
	hash := hashWeights(values)
	if existing, ok := pg.index[hash]; ok {
		return "&" + existing
	}

	pg.index[hash] = varname
	g.P("var ", varname, " = []uint16{")

	for col, w := range values {
		if col > 0 && col%32 == 0 {
			g.WriteByte('\n')
		}
		fmt.Fprintf(g, "0x%04x,", w)
	}
	g.P("}")
	return "&" + varname
}

func WriteFastPage32(g *Generator, varname string, values []uint32) {
	if len(values) != 256 {
		panic("WritePage32: page does not have 256 values")
	}
	g.P("var fast", varname, " = ", Array32(values))
}

type EmbedPageGenerator struct {
	index map[string]string
	raw   bytes.Buffer
}

func hashWeights(values []uint16) string {
	h := sha256.New()
	for _, v := range values {
		h.Write([]byte{byte(v >> 8), byte(v)})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (pg *EmbedPageGenerator) WritePage16(g *Generator, varname string, values []uint16) string {
	hash := hashWeights(values)
	if existing, ok := pg.index[hash]; ok {
		return "&" + existing
	}

	pg.index[hash] = varname

	g.P("var ", varname, " = weightsUCA_embed(", pg.raw.Len()/2, ", ", len(values), ")")

	for _, v := range values {
		pg.raw.WriteByte(byte(v))
		pg.raw.WriteByte(byte(v >> 8))
	}
	return "&" + varname
}

func (pg *EmbedPageGenerator) WriteTrailer(g *Generator, embedfile string) {
	unsafe := Package("unsafe")
	reflect := Package("reflect")
	g.UsePackage("embed")

	g.P()
	g.P("//go:embed ", embedfile)
	g.P("var weightsUCA_embed_data string")
	g.P()
	g.P("func weightsUCA_embed(pos, length int) []uint16 {")
	g.P("return (*[0x7fff0000]uint16)(", unsafe, ".Pointer((*", reflect, ".StringHeader)(", unsafe, ".Pointer(&weightsUCA_embed_data)).Data))[pos:pos+length]")
	g.P("}")
}

func (pg *EmbedPageGenerator) WriteToFile(out string) {
	if err := os.WriteFile(out, pg.raw.Bytes(), 0644); err != nil {
		log.Fatal(err)
	}
	log.Printf("written %q (%.02fkb)", out, float64(pg.raw.Len())/1024.0)
}

type PageGenerator interface {
	WritePage16(g *Generator, varname string, values []uint16) string
}

func NewPageGenerator(embed bool) PageGenerator {
	index := make(map[string]string)
	if embed {
		return &EmbedPageGenerator{index: index}
	}
	return &LiteralPageGenerator{index: index}
}

type entry struct {
	weights []uint16
}

func (e *entry) adjustHangulWeights(tb *TableGenerator, jamos []rune) {
	for _, jamo := range jamos {
		_, entry := tb.entryForCodepoint(jamo)
		e.weights = append(e.weights, entry.weights[0], entry.weights[1], entry.weights[2]+1)
	}
}

type page struct {
	n          int
	entryCount int
	entries    [uca.CodepointsPerPage]entry
}

func (p *page) equals(other *page) bool {
	return reflect.DeepEqual(p, other)
}

func (p *page) name(uca string) string {
	if p.entryCount == 0 {
		panic("cannot name empty page")
	}
	return fmt.Sprintf("weightTable_%s_page%03X", uca, p.n)
}

func (p *page) findMaxCollationElements() int {
	var weightn int
	for _, entry := range p.entries {
		if len(entry.weights) > weightn {
			weightn = len(entry.weights)
		}
	}
	return weightn
}

func (p *page) weights900Fast(level int) (w []uint32) {
	if p.entryCount == 0 {
		return nil
	}
	for i := 0; i < 128; i++ {
		entry := &p.entries[i]
		if len(entry.weights) > 3 {
			panic("trying to dump fast weights for codepoint with >3 weights")
		}
		var weight uint32
		if level < len(entry.weights) {
			weight = uint32(bits.ReverseBytes16(entry.weights[level]))
		}
		if weight != 0 {
			weight |= 0x20000
		}
		w = append(w, weight)
	}
	for i := 0; i < 128; i++ {
		w = append(w, 0x0)
	}
	return
}

func (p *page) weights900() (w []uint16) {
	if p.entryCount == 0 {
		return nil
	}
	maxCollations := p.findMaxCollationElements()
	for _, entry := range p.entries {
		w = append(w, uint16(len(entry.weights)/3))
	}
	for level := 0; level < maxCollations; level++ {
		for _, entry := range p.entries {
			var weight uint16
			if level < len(entry.weights) {
				weight = entry.weights[level]
			}
			w = append(w, weight)
		}
	}
	return
}

func (p *page) weightsLegacy() (w []uint16) {
	if p.entryCount == 0 {
		return nil
	}
	stride := p.findMaxCollationElements()
	w = append(w, uint16(stride))
	for _, entry := range p.entries {
		var i int
		for i < len(entry.weights) {
			w = append(w, entry.weights[i])
			i++
		}
		for i < stride {
			w = append(w, 0x0)
			i++
		}
	}
	return
}

type TableGenerator struct {
	pages   []page
	maxChar rune
	ucav    string
	pg      PageGenerator
}

func (tg *TableGenerator) entryForCodepoint(codepoint rune) (*page, *entry) {
	page := &tg.pages[int(codepoint)/uca.CodepointsPerPage]
	entry := &page.entries[int(codepoint)%uca.CodepointsPerPage]
	return page, entry
}

func (tg *TableGenerator) Add900(codepoint rune, rhs [][3]uint16) {
	page, entry := tg.entryForCodepoint(codepoint)
	page.entryCount++

	for i, weights := range rhs {
		if i >= uca.MaxCollationElementsPerCodepoint {
			break
		}
		for _, we := range weights {
			entry.weights = append(entry.weights, we)
		}
	}
}

func (tg *TableGenerator) Add(codepoint rune, weights []uint16) {
	page, entry := tg.entryForCodepoint(codepoint)
	page.entryCount++

	if entry.weights != nil {
		panic("duplicate codepoint inserted")
	}
	entry.weights = append(entry.weights, weights...)
}

func (tg *TableGenerator) AddFromAllkeys(lhs []rune, rhs [][]int, vars []int) {
	if len(lhs) > 1 || lhs[0] > tg.maxChar {
		// TODO: support contractions
		return
	}

	var weights [][3]uint16
	for _, we := range rhs {
		if len(we) != 3 {
			panic("non-triplet weight in allkeys.txt")
		}
		weights = append(weights, [3]uint16{uint16(we[0]), uint16(we[1]), uint16(we[2])})
	}
	tg.Add900(lhs[0], weights)
}

func (tg *TableGenerator) writePage(g *Generator, p *page, layout uca.Layout) string {
	var weights []uint16

	switch layout.(type) {
	case uca.Layout_uca900:
		weights = p.weights900()
	case uca.Layout_uca_legacy:
		weights = p.weightsLegacy()
	}

	if len(weights) == 0 {
		return "nil"
	}
	return tg.pg.WritePage16(g, p.name(tg.ucav), weights)
}

func (tg *TableGenerator) WriteTables(g *Generator, layout uca.Layout) {
	var pagePtrs []string
	for _, page := range tg.pages {
		pagePtrs = append(pagePtrs, tg.writePage(g, &page, layout))
	}

	g.P("var weightTable_", tg.ucav, " = []*[]uint16{")
	for col, pageptr := range pagePtrs {
		if col > 0 && col%32 == 0 {
			g.WriteByte('\n')
		}
		g.WriteString(pageptr)
		g.WriteByte(',')
	}
	g.P("}")
}

func (tg *TableGenerator) WriteFastTables(g *Generator, layout uca.Layout) {
	switch layout.(type) {
	case uca.Layout_uca900:
	default:
		panic("unsupported table layout for FastTables")
	}

	ascii := &tg.pages[0]
	WriteFastPage32(g, ascii.name(tg.ucav)+"L0", ascii.weights900Fast(0))
	WriteFastPage32(g, ascii.name(tg.ucav)+"L1", ascii.weights900Fast(1))
	WriteFastPage32(g, ascii.name(tg.ucav)+"L2", ascii.weights900Fast(2))
}

func NewTableGenerator(ucav string, pagebuilder PageGenerator) *TableGenerator {
	var maxChar rune
	switch ucav {
	case "uca520", "uca900", "uca900_zh", "uca900_ja":
		maxChar = uca.MaxCodepoint
	case "uca400":
		maxChar = 0xFFFF + 1
	default:
		panic("unknown UCA version")
	}

	tb := &TableGenerator{
		pages:   make([]page, maxChar/uca.CodepointsPerPage),
		maxChar: maxChar,
		ucav:    ucav,
		pg:      pagebuilder,
	}

	for n := range tb.pages {
		tb.pages[n].n = n
	}

	return tb
}
