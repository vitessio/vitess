// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

var (
	ignores = [][]byte{
		[]byte("#"),
		[]byte("/*"),
		[]byte("SET"),
		[]byte("use"),
		[]byte("BEGIN"),
		[]byte("COMMIT"),
		[]byte("ROLLBACK"),
	}
	bindIndex = 0
	queries   = make(map[string]int)
)

type Stat struct {
	Query string
	Count int
}

type Stats []Stat

func (a Stats) Len() int           { return len(a) }
func (a Stats) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Stats) Less(i, j int) bool { return a[i].Count > a[j].Count }

func main() {
	flag.Parse()
	for _, filename := range flag.Args() {
		fmt.Printf("processing: %s\n", filename)
		processFile(filename)
	}
	var stats = make(Stats, 0, 128)
	for k, v := range queries {
		stats = append(stats, Stat{Query: k, Count: v})
	}
	sort.Sort(stats)
	for _, s := range stats {
		fmt.Printf("%d: %s\n", s.Count, s.Query)
	}
}

func processFile(filename string) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		analyze(line)
	}
}

func analyze(line []byte) {
	for _, ignore := range ignores {
		if bytes.HasPrefix(line, ignore) {
			return
		}
	}
	dml := string(bytes.TrimRight(line, "\n"))
	ast, err := sqlparser.Parse(dml)
	if err != nil {
		log.Errorf("Error parsing %s", dml)
		return
	}
	bindIndex = 0
	buf := sqlparser.NewTrackedBuffer(FormatWithBind)
	buf.Myprintf("%v", ast)
	addQuery(buf.ParsedQuery().Query)
}

func FormatWithBind(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch node := node.(type) {
	case sqlparser.StrVal, sqlparser.NumVal:
		buf.WriteArg(fmt.Sprintf("v%d", bindIndex))
		bindIndex++
	default:
		node.Format(buf)
	}
}

func addQuery(query string) {
	count, ok := queries[query]
	if !ok {
		count = 0
	}
	queries[query] = count + 1
}
