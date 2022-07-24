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
	"path"
	"strconv"

	"vitess.io/vitess/go/mysql/collations/internal/uca"
	"vitess.io/vitess/go/mysql/collations/tools/makecolldata/codegen"
)

func maketable(g *codegen.Generator, table string, collation *CollationMetadata, pages codegen.PageGenerator, layout uca.Layout) *codegen.TableGenerator {
	tg := codegen.NewTableGenerator(table, pages)
	for key, weights := range collation.Weights {
		cp, err := strconv.ParseInt(key[2:], 16, 32)
		if err != nil {
			panic(err)
		}
		tg.Add(rune(cp), weights)
	}
	tg.WriteTables(g, layout)
	return tg
}

func maketables(embed bool, output string, metadata AllMetadata) {
	var pages = codegen.NewPageGenerator(embed)
	var g = codegen.NewGenerator("vitess.io/vitess/go/mysql/collations")
	var fastg = codegen.NewGenerator("vitess.io/vitess/go/mysql/collations/internal/uca")

	tablegen := maketable(g, "uca900", metadata.get("utf8mb4_0900_ai_ci"), pages, uca.Layout_uca900{})
	tablegen.WriteFastTables(fastg, uca.Layout_uca900{})

	maketable(g, "uca900_ja", metadata.get("utf8mb4_ja_0900_as_cs"), pages, uca.Layout_uca900{})
	maketable(g, "uca900_zh", metadata.get("utf8mb4_zh_0900_as_cs"), pages, uca.Layout_uca900{})

	maketable(g, "uca400", metadata.get("utf8mb4_unicode_ci"), pages, uca.Layout_uca_legacy{})
	maketable(g, "uca520", metadata.get("utf8mb4_unicode_520_ci"), pages, uca.Layout_uca_legacy{})

	if pages, ok := pages.(*codegen.EmbedPageGenerator); ok {
		pages.WriteTrailer(g, "mysqlucadata.bin")
		pages.WriteToFile(path.Join(output, "mysqlucadata.bin"))
	}

	g.WriteToFile(path.Join(output, "mysqlucadata.go"))
	fastg.WriteToFile(path.Join(output, "internal/uca/fasttables.go"))
}
