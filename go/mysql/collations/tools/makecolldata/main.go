package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"

	"vitess.io/vitess/go/mysql/collations/internal/charset"
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

var Mysqldata = flag.String("mysqldata", "testdata/mysqldata", "")

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
const PkgCharset codegen.Package = "vitess.io/vitess/go/mysql/collations/internal/charset"

func main() {
	flag.Parse()
	metadata := loadMysqlMetadata()
	maketables(".", metadata)
	makeversions(".")
	makemysqldata(".", metadata)
}
