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

package main

import (
	"fmt"
	"log"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/internal/uca"
	"vitess.io/vitess/go/mysql/collations/tools/makecolldata/codegen"
)

const Print8BitData = true

type TableGenerator struct {
	*codegen.Generator
	dedup map[string]string

	baseWeightsUca400 TailoringWeights
	baseWeightsUca520 TailoringWeights
	baseWeightsUca900 TailoringWeights
}

type Generator struct {
	*codegen.Generator
	Tables TableGenerator
}

func diffMaps(orgWeights, modWeights TailoringWeights) (diff []uca.Patch) {
	if len(modWeights) == 0 {
		return nil
	}

	diffMap := make(TailoringWeights)
	for key, val := range modWeights {
		if orgVal, ok := orgWeights[key]; !ok || len(orgVal) != len(val) {
			diffMap[key] = val
			continue
		}

		for i, arr := range val {
			if orgWeights[key][i] != arr {
				diffMap[key] = val
				break
			}
		}
	}

	for key, val := range diffMap {
		cp, err := strconv.ParseInt(key[2:], 16, 32)
		if err != nil {
			panic(err)
		}
		diff = append(diff, uca.Patch{Codepoint: rune(cp), Patch: val})
	}

	sort.Slice(diff, func(i, j int) bool {
		return diff[i].Codepoint < diff[j].Codepoint
	})

	return
}

func (g *TableGenerator) dedupTable(name, coll string, val any) (string, bool) {
	raw := fmt.Sprintf("%#v", val)
	if exist, ok := g.dedup[raw]; ok {
		return exist, true
	}

	varname := fmt.Sprintf("%s_%s", name, coll)
	g.dedup[raw] = varname
	return varname, false
}

func (g *Generator) printCollationUcaLegacy(meta *CollationMetadata) {
	tableWeightPatches := g.Tables.writeWeightPatches(meta)
	tableContractions := g.Tables.writeContractions(meta)

	g.P(meta.Number, ": &Collation_uca_legacy{")
	g.P("name: ", codegen.Quote(meta.Name), ",")
	g.P("id: ", meta.Number, ",")

	var maxCodepoint uint
	switch meta.UCAVersion {
	case 400:
		maxCodepoint = 0xFFFF
	case 520:
		maxCodepoint = 0x10FFFF
	default:
		g.Fail("invalid UCAVersion")
	}

	g.P("uca: uca.NewCollationLegacy(",
		PkgCharset, ".Charset_", meta.Charset, "{},",
		"weightTable_uca", meta.UCAVersion, ",",
		or(tableWeightPatches, "nil"), ",",
		or(tableContractions, "nil"), ",",
		maxCodepoint, "),",
	)

	// The case table is the MY_UNICASE_INFO that the collation points at
	// in the MySQL source. The dump does not have it. The Turkish
	// collations use my_unicase_turkish, the Unicode 5.2.0 collations use
	// my_unicase_unicode520 and the others use my_unicase_default.
	switch {
	case strings.Contains(meta.Name, "_turkish_"):
		g.P("unicase: unicaseInfo_turkish,")
	case meta.UCAVersion == 520:
		g.P("unicase: unicaseInfo_unicode520,")
	default:
		g.P("unicase: unicaseInfo_default,")
	}
	g.P("},")
}

func (g *TableGenerator) writeWeightPatches(meta *CollationMetadata) string {
	var tableWeightPatches string
	var dedup bool
	var baseWeights TailoringWeights

	switch meta.UCAVersion {
	case 400:
		baseWeights = g.baseWeightsUca400
	case 520:
		baseWeights = g.baseWeightsUca520
	case 900:
		baseWeights = g.baseWeightsUca900
	default:
		g.Fail("invalid UCAVersion")
	}

	diff := diffMaps(baseWeights, meta.Weights)
	if len(diff) > 0 {
		tableWeightPatches, dedup = g.dedupTable("weightTailoring", meta.Name, diff)
		if !dedup {
			g.P("var ", tableWeightPatches, " = ", diff)
			g.P()
		}
	}

	return tableWeightPatches
}

func (g *TableGenerator) writeContractions(meta *CollationMetadata) string {
	if len(meta.Contractions) > 0 {
		tableContractions, dedup := g.dedupTable("contractor", meta.Name, meta.Contractions)
		if !dedup {
			g.printContractionsFast(tableContractions, meta.Contractions)
			g.P()
		}
		return tableContractions + "{}"
	}
	return ""
}

func (g *TableGenerator) writeReorders(meta *CollationMetadata) string {
	var tableReorder string
	var dedup bool

	if len(meta.Reorder) > 0 {
		tableReorder, dedup = g.dedupTable("reorder", meta.Name, meta.Reorder)
		if !dedup {
			reorder := make([]uca.Reorder, 0, len(meta.Reorder))
			for _, r := range meta.Reorder {
				reorder = append(reorder, uca.Reorder{FromMin: r[0], FromMax: r[1], ToMin: r[2], ToMax: r[3]})
			}
			g.P("var ", tableReorder, " = ", reorder)
			g.P()
		}
	}
	return tableReorder
}

func (g *Generator) printCollationUca900(meta *CollationMetadata) {
	if meta.UCAVersion != 900 {
		g.Fail("unexpected UCA version for UCA900 collation")
	}

	tableWeights := "weightTable_uca900"
	switch meta.Name {
	case "utf8mb4_zh_0900_as_cs":
		// the chinese weights table is large enough that we don't apply weight patches
		// to it, we generate it as a whole
		tableWeights = "weightTable_uca900_zh"
		meta.Weights = nil

		// HACK: Chinese collations are fully reordered on their patched weights.
		// They do not need manual reordering even if they include reorder ranges
		// FIXME: Why does this collation have a reorder range that doesn't apply?
		meta.Reorder = nil

	case "utf8mb4_ja_0900_as_cs", "utf8mb4_ja_0900_as_cs_ks":
		// the japanese weights table is large enough that we don't apply weight patches
		// to it, we generate it as a whole
		tableWeights = "weightTable_uca900_ja"
		meta.Weights = nil
	}

	tableWeightPatches := g.Tables.writeWeightPatches(meta)
	tableContractions := g.Tables.writeContractions(meta)
	tableReorder := g.Tables.writeReorders(meta)
	name := codegen.Quote(meta.Name)

	g.P(meta.Number, ": &Collation_utf8mb4_uca_0900{")
	g.P("name: ", name, ",")
	g.P("id: ", meta.Number, ",")

	var levels int
	switch {
	case strings.HasSuffix(meta.Name, "_ai_ci"):
		levels = 1
	case strings.HasSuffix(meta.Name, "_as_ci"):
		levels = 2
	case strings.HasSuffix(meta.Name, "_as_cs"):
		levels = 3
	case strings.HasSuffix(meta.Name, "_as_cs_ks"):
		levels = 4
	default:
		g.Fail(fmt.Sprintf("unknown levelsForCompare: %q", meta.Name))
	}
	g.P("uca: uca.NewCollation(",
		name, ",",
		tableWeights, ",",
		or(tableWeightPatches, "nil"), ",",
		or(tableReorder, "nil"), ",",
		or(tableContractions, "nil"), ",",
		meta.UpperCaseFirst, ",",
		levels, "),",
	)
	g.P("},")
}

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (g *TableGenerator) printSlice(name, coll string, slice any) string {
	tableName, dedup := g.dedupTable(name, coll, slice)
	if !dedup {
		g.P("var ", tableName, " = ", slice)
		g.P()
	}
	return tableName
}

func (g *TableGenerator) printUnicodeMappings(name, coll string, mappings []charset.UnicodeMapping) string {
	tableName, dedup := g.dedupTable(name, coll, mappings)
	if !dedup {
		g.P("var ", tableName, " = ", mappings)
		g.P()
	}
	return tableName
}

func (g *Generator) printCollation8bit(meta *CollationMetadata) {
	var tableCtype, tableToLower, tableToUpper, tableSortOrder, tableToUnicode, tableFromUnicode string

	if Print8BitData {
		tableCtype = g.Tables.printSlice("ctype", meta.Name, codegen.Array8(meta.CType))
		tableToLower = g.Tables.printSlice("tolower", meta.Name, codegen.Array8(meta.ToLower))
		tableToUpper = g.Tables.printSlice("toupper", meta.Name, codegen.Array8(meta.ToUpper))
	}
	if meta.SortOrder != nil {
		tableSortOrder = g.Tables.printSlice("sortorder", meta.Name, codegen.Array8(meta.SortOrder))
	}
	if meta.Charset != "latin1" {
		if meta.TabToUni != nil {
			toUni := meta.TabToUni
			if meta.Name == "latin2_czech_cs" {
				// The table of latin2_czech_cs has no character at 7F,
				// but MySQL copies the bytes below 80 when it converts
				// to an ASCII-compatible character set. Measured on
				// MySQL 8.0.46 and 8.4.13: CONVERT((CONVERT(0x7F USING
				// latin2) COLLATE latin2_czech_cs) USING utf8mb4) is 7F,
				// and so is that column in a result with
				// character_set_results=utf8mb4. Vitess converts results
				// to utf8mb4, so 7F decodes to U+007F here. MySQL gives
				// 003F for utf16, and U+007F stays without a byte in
				// latin2_czech_cs: an INSERT of it stores "?".
				toUni = slices.Clone(toUni)
				toUni[0x7F] = 0x7F
			}
			tableToUnicode = g.Tables.printSlice("tounicode", meta.Name, codegen.Array16(toUni))
		}
		if meta.TabFromUni != nil {
			tableFromUnicode = g.Tables.printUnicodeMappings("fromunicode", meta.Name, meta.TabFromUni)
		}
	}

	var collation string
	switch {
	case meta.Flags.Binary:
		collation = "Collation_8bit_bin"
	case meta.Name == "latin1_german2_ci":
		// The collation has the tables of an 8-bit collation, and
		// its own comparison in the MySQL source.
		collation = "Collation_latin1_german2_ci"
	case meta.Name == "tis620_thai_ci":
		// As latin1_german2_ci.
		collation = "Collation_tis620_thai_ci"
	case meta.Name == "latin2_czech_cs":
		// As latin1_german2_ci. The tables of its four passes are
		// in colldata/czech_data.go.
		collation = "Collation_latin2_czech_cs"
	case meta.Name == "cp1250_czech_cs":
		// As latin2_czech_cs, with two passes.
		collation = "Collation_cp1250_czech_cs"
	default:
		collation = "Collation_8bit_simple_ci"
	}

	g.P(meta.Number, ": &", collation, "{")
	g.P("id: ", meta.Number, ",")
	g.P("name: ", codegen.Quote(meta.Name), ",")

	g.P("simpletables: simpletables{")
	if Print8BitData {
		g.P("ctype: &", tableCtype, ",")
		g.P("tolower: &", tableToLower, ",")
		g.P("toupper: &", tableToUpper, ",")
	}
	if tableSortOrder != "" {
		g.P("sort: &", tableSortOrder, ",")
	}
	g.P("},")

	// Optimized implementation for latin1
	switch {
	case meta.Charset == "latin1":
		g.P("charset: ", PkgCharset, ".Charset_latin1{},")
	case meta.Name == "tis620_thai_ci":
		// Collation_tis620_thai_ci always has the tis620 character
		// set.
	case meta.Charset == "tis620":
		// MySQL converts tis620 with code and not with the tables
		// that the dump has for the other 8-bit character sets.
		g.P("charset: ", PkgCharset, ".Charset_tis620{},")
	default:
		g.P("charset: &", PkgCharset, ".Charset_8bit{")
		g.P("Name_: ", codegen.Quote(meta.Charset), ",")
		if tableToUnicode != "" {
			g.P("ToUnicode: &", tableToUnicode, ",")
		}
		if tableFromUnicode != "" {
			g.P("FromUnicode: ", tableFromUnicode, ",")
		}
		g.P("},")
	}
	g.P("},")
}

func (g *Generator) printCollationUnicode(meta *CollationMetadata) {
	var collation string
	if meta.Flags.Binary {
		collation = "Collation_unicode_bin"
	} else {
		collation = "Collation_unicode_general_ci"
	}
	g.P(meta.Number, ": &", collation, "{")
	g.P("id: ", meta.Number, ",")
	g.P("name: ", strconv.Quote(meta.Name), ",")
	// The case table is the MY_UNICASE_INFO that the collation points at
	// in the MySQL source, and for utf8mb3_tolower_ci the default table
	// with the MY_CS_LOWER_SORT flag of that collation. The dump has
	// neither.
	switch {
	case meta.Flags.Binary:
	case strings.HasSuffix(meta.Name, "_general_mysql500_ci"):
		g.P("unicase: unicaseInfo_mysql500,")
	case meta.Name == "utf8mb3_tolower_ci":
		g.P("unicase: unicaseInfo_tolower,")
	default:
		g.P("unicase: unicaseInfo_default,")
	}
	g.P("charset: ", PkgCharset, ".Charset_", meta.Charset, "{},")
	g.P("},")
}

func (g *Generator) printCollationMultibyte(meta *CollationMetadata) {
	var tableSortOrder string
	if meta.SortOrder != nil {
		tableSortOrder = g.Tables.printSlice("sortorder", meta.Name, codegen.Array8(meta.SortOrder))
	}

	g.P(meta.Number, ": &Collation_multibyte{")
	g.P("id: ", meta.Number, ",")
	g.P("name: ", codegen.Quote(meta.Name), ",")
	if tableSortOrder != "" {
		g.P("sort: &", tableSortOrder, ",")
	}
	g.P("charset: ", PkgCharset, ".Charset_", meta.Charset, "{},")
	// The case table of the character set, my_caseinfo_<charset> in the
	// MySQL source. The dump does not have it.
	if meta.Charset == "gb18030" {
		g.P("caseinfo: gb18030Case{},")
	} else {
		g.P("caseinfo: multibyteCase_", meta.Charset, ",")
	}
	g.P("},")
}

// printCollationChineseCI prints gbk_chinese_ci and big5_chinese_ci. The dump
// has their sort tables for single bytes. The order of two-byte characters
// is in colldata/gbk_order.go and colldata/chinese_ci.go.
func (g *Generator) printCollationChineseCI(meta *CollationMetadata) {
	tableSortOrder := g.Tables.printSlice("sortorder", meta.Name, codegen.Array8(meta.SortOrder))

	g.P(meta.Number, ": &Collation_chinese_ci{")
	g.P("id: ", meta.Number, ",")
	g.P("name: ", codegen.Quote(meta.Name), ",")
	g.P("sort: &", tableSortOrder, ",")
	g.P("charset: ", PkgCharset, ".Charset_", meta.Charset, "{},")
	g.P("caseinfo: multibyteCase_", meta.Charset, ",")
	switch meta.Charset {
	case "gbk":
		g.P("charLen: charLenGBK,")
		g.P("order: gbkSortOrder,")
		g.P("weight: gbkSortOrder,")
	case "big5":
		g.P("charLen: charLenBig5,")
		g.P("order: big5Code,")
		g.P("weight: big5StrokeWeight,")
	default:
		g.Fail("no order for " + meta.Charset)
	}
	g.P("},")
}

// printCollationGB18030 prints gb18030_chinese_ci. The dump has its sort
// table for single bytes. The weights of multi-byte characters are in
// colldata/gb18030_data.go.
func (g *Generator) printCollationGB18030(meta *CollationMetadata) {
	tableSortOrder := g.Tables.printSlice("sortorder", meta.Name, codegen.Array8(meta.SortOrder))

	g.P(meta.Number, ": &Collation_gb18030_chinese_ci{")
	g.P("id: ", meta.Number, ",")
	g.P("name: ", codegen.Quote(meta.Name), ",")
	g.P("sort: &", tableSortOrder, ",")
	g.P("},")
}

func makemysqldata(output string, supportedOutput string, metadata AllMetadata) {
	unsupportedByCharset := make(map[string][]string)
	g := Generator{
		Generator: codegen.NewGenerator(PkgCollationsData),
		Tables: TableGenerator{
			Generator:         codegen.NewGenerator(PkgCollationsData),
			dedup:             make(map[string]string),
			baseWeightsUca400: metadata.get("utf8mb4_unicode_ci").Weights,
			baseWeightsUca520: metadata.get("utf8mb4_unicode_520_ci").Weights,
			baseWeightsUca900: metadata.get("utf8mb4_0900_ai_ci").Weights,
		},
	}

	h := Generator{
		Generator: codegen.NewGenerator("vitess.io/vitess/go/mysql/collations"),
	}

	g.P("var collationsById = [...]Collation{")
	h.P("var supported = [...]string{")

	for _, meta := range metadata {
		switch {
		case meta.Name == "utf8mb4_0900_bin":
			g.P(uint(309), ": &Collation_utf8mb4_0900_bin{},")
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		case meta.Name == "binary":
			g.P(uint(63), ": &Collation_binary{},")
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		case meta.CollationImpl == "any_uca" ||
			meta.CollationImpl == "utf16_uca" ||
			meta.CollationImpl == "utf32_uca" ||
			meta.CollationImpl == "ucs2_uca":
			g.printCollationUcaLegacy(meta)
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		case meta.CollationImpl == "uca_900":
			g.printCollationUca900(meta)
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		case meta.CollationImpl == "8bit_bin" || meta.CollationImpl == "8bit_simple_ci" || meta.Name == "latin1_german2_ci" || meta.Name == "tis620_thai_ci" ||
			meta.Name == "latin2_czech_cs" || meta.Name == "cp1250_czech_cs":
			g.printCollation8bit(meta)
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		case meta.Name == "gb18030_unicode_520_ci":
			g.printCollationUcaLegacy(meta)
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		case meta.Name == "gbk_chinese_ci", meta.Name == "big5_chinese_ci":
			g.printCollationChineseCI(meta)
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		case meta.Name == "gb18030_chinese_ci":
			g.printCollationGB18030(meta)
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		case charset.IsMultibyteByName(meta.Charset), meta.Name == "gb18030_bin":
			g.printCollationMultibyte(meta)
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		case strings.HasSuffix(meta.Name, "_bin") && charset.IsUnicodeByName(meta.Charset):
			g.printCollationUnicode(meta)
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		case strings.HasSuffix(meta.Name, "_general_ci"),
			strings.HasSuffix(meta.Name, "_general_mysql500_ci"),
			meta.Name == "utf8mb3_tolower_ci":
			g.printCollationUnicode(meta)
			h.P(meta.Number, ": ", codegen.Quote(meta.Name), ",")

		default:
			unsupportedByCharset[meta.Charset] = append(unsupportedByCharset[meta.Charset], meta.Name)
		}
	}

	g.P("}")
	h.P("}")
	codegen.Merge(g.Tables.Generator, g.Generator).WriteToFile(path.Join(output, "mysqldata.go"))
	h.WriteToFile(path.Join(supportedOutput, "supported.go"))

	var unhandledCount int
	for impl, collations := range unsupportedByCharset {
		log.Printf("unhandled implementation %q: %s", impl, strings.Join(collations, ", "))
		unhandledCount += len(collations)
	}

	log.Printf("mysqldata: %d/%d collations (%.2f%% handled)",
		len(metadata)-unhandledCount, len(metadata),
		float64(len(metadata)-unhandledCount)/float64(len(metadata))*100.0,
	)
}
