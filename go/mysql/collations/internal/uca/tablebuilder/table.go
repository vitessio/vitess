package tablebuilder

import (
	"fmt"
	"io"
	"reflect"

	"vitess.io/vitess/go/mysql/collations/internal/uca"
)

const MaxColumns = 16

type entry struct {
	weights []uint16
}

func (e *entry) adjustHangulWeights(tb *TableBuilder, jamos []rune) {
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

func (p *page) adjustImplicitWeights(tb *TableBuilder) {
	if p.entryCount == uca.CodepointsPerPage {
		return
	}

	baseCodepoint := p.n * uca.CodepointsPerPage
	for n := range p.entries {
		codepoint := rune(baseCodepoint + n)
		entry := &p.entries[n]
		if len(entry.weights) > 0 {
			continue
		}

		if jamos := uca.UnicodeDecomposeHangulSyllable(codepoint); jamos != nil {
			entry.adjustHangulWeights(tb, jamos)
			continue
		}

		entry.weights = make([]uint16, 6)
		uca.UnicodeImplicitWeights900(entry.weights, codepoint)
	}
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

type TableBuilder struct {
	pages       []page
	maxChar     rune
	ucav        string
	pagebuilder *EmbeddedPageBuilder
}

func (tb *TableBuilder) entryForCodepoint(codepoint rune) (*page, *entry) {
	page := &tb.pages[int(codepoint)/uca.CodepointsPerPage]
	entry := &page.entries[int(codepoint)%uca.CodepointsPerPage]
	return page, entry
}

func (tb *TableBuilder) Add900(codepoint rune, rhs [][3]uint16) {
	page, entry := tb.entryForCodepoint(codepoint)
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

func (tb *TableBuilder) Add(codepoint rune, weights []uint16) {
	page, entry := tb.entryForCodepoint(codepoint)
	page.entryCount++

	if entry.weights != nil {
		panic("duplicate codepoint inserted")
	}
	entry.weights = append(entry.weights, weights...)
}

func (tb *TableBuilder) AddFromAllkeys(lhs []rune, rhs [][]int, vars []int) {
	if len(lhs) > 1 || lhs[0] > tb.maxChar {
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
	tb.Add900(lhs[0], weights)
}

func (tb *TableBuilder) dumpPage(w io.Writer, p *page, layout uca.TableLayout) string {
	var weights []uint16

	switch layout.(type) {
	case uca.TableLayout_uca900:
		weights = p.weights900()
	case uca.TableLayout_uca_legacy:
		weights = p.weightsLegacy()
	}

	if len(weights) == 0 {
		return "nil"
	}
	return tb.pagebuilder.WritePage(w, p.name(tb.ucav), weights)
}

func (tb *TableBuilder) DumpTables(w io.Writer, layout uca.TableLayout) {
	switch layout.(type) {
	case uca.TableLayout_uca900:
		for n := range tb.pages {
			tb.pages[n].adjustImplicitWeights(tb)
		}
	}

	var pagePtrs []string
	for _, page := range tb.pages {
		pagePtrs = append(pagePtrs, tb.dumpPage(w, &page, layout))
	}

	fmt.Fprintf(w, "var WeightTable_%s = []*[]uint16{", tb.ucav)
	for n, pageptr := range pagePtrs {
		if n%MaxColumns == 0 {
			fmt.Fprintf(w, "\n\t")
		} else {
			fmt.Fprintf(w, " ")
		}
		fmt.Fprintf(w, "%s,", pageptr)
	}
	fmt.Fprintf(w, "\n}\n\n")
}

func NewTableBuilder(ucav string, pagebuilder *EmbeddedPageBuilder) *TableBuilder {
	var maxChar rune
	switch ucav {
	case "uca520", "uca900", "uca900_zh", "uca900_ja":
		maxChar = uca.MaxCodepoint
	case "uca400":
		maxChar = 0xFFFF + 1
	default:
		panic("unknown UCA version")
	}

	tb := &TableBuilder{
		pages:       make([]page, maxChar/uca.CodepointsPerPage),
		maxChar:     maxChar,
		ucav:        ucav,
		pagebuilder: pagebuilder,
	}

	for n := range tb.pages {
		tb.pages[n].n = n
	}

	return tb
}
