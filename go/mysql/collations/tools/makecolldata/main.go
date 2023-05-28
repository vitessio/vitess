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
	"encoding/json"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/internal/uca"
	"vitess.io/vitess/go/mysql/collations/tools/makecolldata/codegen"
)

type TailoringWeights map[string][]uint16

type AllMetadata []*CollationMetadata

type CollationMetadata struct {
	Name    string
	Charset string
	Flags   struct {
		Binary  bool
		ASCII   bool
		Default bool
	}
	CollationImpl  string
	Number         uint
	CType          []byte
	ToLower        []byte
	ToUpper        []byte
	SortOrder      []byte
	TabToUni       []uint16
	TabFromUni     []charset.UnicodeMapping
	UCAVersion     int
	Weights        TailoringWeights
	Contractions   []uca.Contraction
	Reorder        [][4]uint16
	UpperCaseFirst bool
}

var Mysqldata = pflag.String("mysqldata", "testdata/mysqldata", "")
var Embed = pflag.Bool("embed", false, "")

func loadMysqlMetadata() (all AllMetadata) {
	mysqdata, err := filepath.Glob(path.Join(*Mysqldata, "*.json"))
	if err != nil {
		log.Fatal(err)
	}

	if len(mysqdata) == 0 {
		log.Fatalf("no files under %q (did you run colldump locally?)", *Mysqldata)
	}

	for _, path := range mysqdata {
		rf, err := os.Open(path)
		if err != nil {
			log.Fatal(err)
		}

		var meta CollationMetadata
		if err := json.NewDecoder(rf).Decode(&meta); err != nil {
			log.Fatal(err)
		}
		_ = rf.Close()

		if _, aliased := CharsetAliases[meta.Charset]; aliased {
			meta.Charset = CharsetAliases[meta.Charset]
		}

		all = append(all, &meta)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Number < all[j].Number
	})
	return
}

func (all AllMetadata) get(name string) *CollationMetadata {
	for _, meta := range all {
		if meta.Name == name {
			return meta
		}
	}
	log.Fatalf("missing collation: %s", name)
	return nil
}

const PkgCollations codegen.Package = "vitess.io/vitess/go/mysql/collations"
const PkgCharset codegen.Package = "vitess.io/vitess/go/mysql/collations/charset"

func main() {
	pflag.Parse()
	metadata := loadMysqlMetadata()
	maketables(*Embed, ".", metadata)
	makeversions(".")
	makemysqldata(".", metadata)
}
