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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"log"
	"os"
	"strconv"

	"vitess.io/vitess/go/mysql/collations/uca/tablebuilder"
)

var Output = flag.String("out", "uca", "")

func maketable(table string, filename string, base *tablebuilder.TableBuilder, legacy bool) *tablebuilder.TableBuilder {
	var metadata struct {
		Weights map[string][]uint16
	}

	r, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	err = json.NewDecoder(r).Decode(&metadata)
	if err != nil {
		log.Fatal(err)
	}

	tb := tablebuilder.NewTableBuilder(table, base)

	for key, weights := range metadata.Weights {
		r, err := strconv.ParseInt(key[2:], 16, 32)
		if err != nil {
			log.Fatal(err)
		}
		tb.Add(rune(r), weights)
	}

	var buf bytes.Buffer
	if legacy {
		tb.DumpTablesLegacy(&buf)
	} else {
		tb.DumpTables900(&buf)
	}

	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		fmt.Fprintf(os.Stderr, "source:\n%s\n", buf.String())
		log.Fatalf("failed to format generated code: %v", err)
	}

	output := fmt.Sprintf("%s/tables_%s.go", *Output, table)
	err = os.WriteFile(output, formatted, 0644)
	if err != nil {
		log.Fatalf("failed to generate %q: %v", output, err)
	}

	return tb
}

func main() {
	flag.Parse()

	basetable := maketable("uca900", "testdata/mysqldata/utf8mb4_0900_ai_ci.json", nil, false)
	_ = maketable("uca900_ja", "testdata/mysqldata/utf8mb4_ja_0900_as_cs.json", basetable, false)
	_ = maketable("uca900_zh", "testdata/mysqldata/utf8mb4_zh_0900_as_cs.json", basetable, false)

	_ = maketable("uca400", "testdata/mysqldata/utf8mb4_unicode_ci.json", nil, true)
	_ = maketable("uca520", "testdata/mysqldata/utf8mb4_unicode_520_ci.json", nil, true)
}
