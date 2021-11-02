package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql/collations/internal/codegen"
)

type versionInfo struct {
	alias     map[string]byte
	isdefault byte
}

var Output = flag.String("out", "mysqlversion.go", "")

func main() {
	flag.Parse()

	versionfiles, err := filepath.Glob("testdata/versions/collations_*.csv")
	if err != nil {
		log.Fatal(err)
	}
	sort.Strings(versionfiles)

	versioninfo := make(map[uint]*versionInfo)
	for v, versionCsv := range versionfiles {
		f, err := os.Open(versionCsv)
		if err != nil {
			log.Fatal(err)
		}

		scan := bufio.NewScanner(f)
		var row int
		for scan.Scan() {
			if row == 0 {
				row++
				continue
			}

			cols := strings.Split(scan.Text(), "\t")
			collid, err := strconv.ParseUint(cols[2], 10, 16)
			if err != nil {
				log.Fatal(err)
			}

			vi := versioninfo[uint(collid)]
			if vi == nil {
				vi = &versionInfo{alias: make(map[string]byte)}
				versioninfo[uint(collid)] = vi
			}

			vi.alias[cols[0]] |= 1 << v
			switch cols[3] {
			case "Yes":
				vi.isdefault |= 1 << v
			case "No", "":
			default:
				log.Fatalf("unknown value for IS_DEFAULT: %q", cols[3])
			}

			row++
		}
	}

	var versions []string
	for _, versionCsv := range versionfiles {
		base := filepath.Base(versionCsv)
		base = strings.TrimPrefix(base, "collations_")
		base = strings.TrimSuffix(base, ".csv")
		versions = append(versions, base)
	}

	var out = codegen.NewGoFile(*Output)
	fmt.Fprintf(out, "package collations\n\n")
	fmt.Fprintf(out, "type Version byte\n")
	fmt.Fprintf(out, "const (\n")

	for v, version := range versions {
		if v == 0 {
			fmt.Fprintf(out, "Version%s Version = iota\n", version)
		} else {
			fmt.Fprintf(out, "Version%s\n", version)
		}
	}
	fmt.Fprintf(out, ")\n\n")
	fmt.Fprintf(out, "func (v Version) String() string {\n")
	fmt.Fprintf(out, "switch v {\n")
	for _, version := range versions {
		fmt.Fprintf(out, "case Version%s: return %q\n", version, version)
	}
	fmt.Fprintf(out, "default: panic(\"invalid version identifier\")\n")
	fmt.Fprintf(out, "}\n}\n\n")
	fmt.Fprintf(out, "var globalVersionInfo = map[ID]struct{alias map[byte]string; isdefault byte}{\n")

	for collid, vi := range versioninfo {
		reverse := make(map[byte]string)
		for alias, m := range vi.alias {
			reverse[m] = alias
		}
		fmt.Fprintf(out, "%d: {alias: %#v, isdefault: 0x%02x},\n", collid, reverse, vi.isdefault)
	}
	fmt.Fprintf(out, "}\n")

	if err := out.Close(); err != nil {
		log.Fatal(err)
	}
}
