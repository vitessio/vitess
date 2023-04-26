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
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/mysql/collations/internal/uca"
	"vitess.io/vitess/go/mysql/collations/tools/makecolldata/codegen"
)

var Print8BitData = pflag.Bool("full8bit", false, "")

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

	g.P("register(&Collation_uca_legacy{")
	g.P("name: ", codegen.Quote(meta.Name), ",")
	g.P("id: ", meta.Number, ",")
	g.P("charset: ", PkgCharset, ".Charset_", meta.Charset, "{},")
	g.P("weights: weightTable_uca", meta.UCAVersion, ",")
	if tableWeightPatches != "" {
		g.P("tailoring: ", tableWeightPatches, ",")
	}
	if tableContractions != "" {
		g.P("contract: ", tableContractions, "{},")
	}
	switch meta.UCAVersion {
	case 400:
		g.P("maxCodepoint: 0xFFFF,")
	case 520:
		g.P("maxCodepoint: 0x10FFFF,")
	default:
		g.Fail("invalid UCAVersion")
	}
	g.P("})")
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
	var tableContractions string
	var dedup bool

	if len(meta.Contractions) > 0 {
		tableContractions, dedup = g.dedupTable("contractor", meta.Name, meta.Contractions)
		if !dedup {
			g.printContractionsFast(tableContractions, meta.Contractions)
			g.P()
		}
	}
	return tableContractions
}

func (g *TableGenerator) writeReorders(meta *CollationMetadata) string {
	var tableReorder string
	var dedup bool

	if len(meta.Reorder) > 0 {
		tableReorder, dedup = g.dedupTable("reorder", meta.Name, meta.Reorder)
		if !dedup {
			var reorder []uca.Reorder
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

	g.P("register(&Collation_utf8mb4_uca_0900{")
	g.P("name: ", codegen.Quote(meta.Name), ",")
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

	g.P("levelsForCompare: ", levels, ",")
	g.P("weights: ", tableWeights, ",")
	if tableWeightPatches != "" {
		g.P("tailoring: ", tableWeightPatches, ",")
	}
	if tableContractions != "" {
		g.P("contract: ", tableContractions, "{},")
	}
	if tableReorder != "" {
		g.P("reorder: ", tableReorder, ",")
	}
	if meta.UpperCaseFirst {
		g.P("upperCaseFirst: true,")
	}
	g.P("})")
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

	if *Print8BitData {
		tableCtype = g.Tables.printSlice("ctype", meta.Name, codegen.Array8(meta.CType))
		tableToLower = g.Tables.printSlice("tolower", meta.Name, codegen.Array8(meta.ToLower))
		tableToUpper = g.Tables.printSlice("toupper", meta.Name, codegen.Array8(meta.ToUpper))
	}
	if meta.SortOrder != nil {
		tableSortOrder = g.Tables.printSlice("sortorder", meta.Name, codegen.Array8(meta.SortOrder))
	}
	if meta.Charset != "latin1" {
		if meta.TabToUni != nil {
			tableToUnicode = g.Tables.printSlice("tounicode", meta.Name, codegen.Array16(meta.TabToUni))
		}
		if meta.TabFromUni != nil {
			tableFromUnicode = g.Tables.printUnicodeMappings("fromunicode", meta.Name, meta.TabFromUni)
		}
	}

	var collation string
	if meta.Flags.Binary {
		collation = "Collation_8bit_bin"
	} else {
		collation = "Collation_8bit_simple_ci"
	}

	g.P("register(&", collation, "{")
	g.P("id: ", meta.Number, ",")
	g.P("name: ", codegen.Quote(meta.Name), ",")

	g.P("simpletables: simpletables{")
	if *Print8BitData {
		g.P("ctype: &", tableCtype, ",")
		g.P("tolower: &", tableToLower, ",")
		g.P("toupper: &", tableToUpper, ",")
	}
	if tableSortOrder != "" {
		g.P("sort: &", tableSortOrder, ",")
	}
	g.P("},")

	// Optimized implementation for latin1
	if meta.Charset == "latin1" {
		g.P("charset: ", PkgCharset, ".Charset_latin1{},")
	} else {
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
	g.P("})")
}

func (g *Generator) printCollationUnicode(meta *CollationMetadata) {
	var collation string
	if meta.Flags.Binary {
		collation = "Collation_unicode_bin"
	} else {
		collation = "Collation_unicode_general_ci"
	}
	g.P("register(&", collation, "{")
	g.P("id: ", meta.Number, ",")
	g.P("name: ", strconv.Quote(meta.Name), ",")
	if !meta.Flags.Binary {
		g.P("unicase: unicaseInfo_default,")
	}
	g.P("charset: ", PkgCharset, ".Charset_", meta.Charset, "{},")
	g.P("})")
}

func (g *Generator) printCollationMultibyte(meta *CollationMetadata) {
	var tableSortOrder string
	if meta.SortOrder != nil {
		tableSortOrder = g.Tables.printSlice("sortorder", meta.Name, codegen.Array8(meta.SortOrder))
	}

	g.P("register(&Collation_multibyte{")
	g.P("id: ", meta.Number, ",")
	g.P("name: ", codegen.Quote(meta.Name), ",")
	if tableSortOrder != "" {
		g.P("sort: &", tableSortOrder, ",")
	}
	g.P("charset: ", PkgCharset, ".Charset_", meta.Charset, "{},")
	g.P("})")
}

func makemysqldata(output string, metadata AllMetadata) {
	var unsupportedByCharset = make(map[string][]string)
	var g = Generator{
		Generator: codegen.NewGenerator(PkgCollations),
		Tables: TableGenerator{
			Generator:         codegen.NewGenerator(PkgCollations),
			dedup:             make(map[string]string),
			baseWeightsUca400: metadata.get("utf8mb4_unicode_ci").Weights,
			baseWeightsUca520: metadata.get("utf8mb4_unicode_520_ci").Weights,
			baseWeightsUca900: metadata.get("utf8mb4_0900_ai_ci").Weights,
		},
	}

	g.P("func init() {")

	for _, meta := range metadata {
		switch {
		case meta.Name == "utf8mb4_0900_bin" || meta.Name == "binary":
			// hardcoded collations; nothing to export here

		case meta.Name == "tis620_bin":
			// explicitly unsupported for now because of not accurate results

		case meta.CollationImpl == "any_uca" ||
			meta.CollationImpl == "utf16_uca" ||
			meta.CollationImpl == "utf32_uca" ||
			meta.CollationImpl == "ucs2_uca":
			g.printCollationUcaLegacy(meta)

		case meta.CollationImpl == "uca_900":
			g.printCollationUca900(meta)

		case meta.CollationImpl == "8bit_bin" || meta.CollationImpl == "8bit_simple_ci":
			g.printCollation8bit(meta)

		case meta.Name == "gb18030_unicode_520_ci":
			g.printCollationUcaLegacy(meta)

		case charset.IsMultibyteByName(meta.Charset):
			g.printCollationMultibyte(meta)

		case strings.HasSuffix(meta.Name, "_bin") && charset.IsUnicodeByName(meta.Charset):
			g.printCollationUnicode(meta)

		case strings.HasSuffix(meta.Name, "_general_ci"):
			g.printCollationUnicode(meta)

		default:
			unsupportedByCharset[meta.Charset] = append(unsupportedByCharset[meta.Charset], meta.Name)
		}
	}

	g.P("}")
	codegen.Merge(g.Tables.Generator, g.Generator).WriteToFile(path.Join(output, "mysqldata.go"))

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
