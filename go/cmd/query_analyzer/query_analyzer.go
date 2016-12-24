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
	"github.com/youtube/vitess/go/exit"
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

type stat struct {
	Query string
	Count int
}

type stats []stat

func (a stats) Len() int           { return len(a) }
func (a stats) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a stats) Less(i, j int) bool { return a[i].Count > a[j].Count }

func main() {
	defer exit.Recover()
	flag.Parse()
	for _, filename := range flag.Args() {
		fmt.Printf("processing: %s\n", filename)
		if err := processFile(filename); err != nil {
			log.Errorf("processFile error: %v", err)
			exit.Return(1)
		}
	}
	var stats = make(stats, 0, 128)
	for k, v := range queries {
		stats = append(stats, stat{Query: k, Count: v})
	}
	sort.Sort(stats)
	for _, s := range stats {
		fmt.Printf("%d: %s\n", s.Count, s.Query)
	}
}

func processFile(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		analyze(line)
	}
	return nil
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
	buf := sqlparser.NewTrackedBuffer(formatWithBind)
	buf.Myprintf("%v", ast)
	addQuery(buf.ParsedQuery().Query)
}

func formatWithBind(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	v, ok := node.(*sqlparser.SQLVal)
	if !ok {
		node.Format(buf)
		return
	}
	switch v.Type {
	case sqlparser.StrVal, sqlparser.HexVal, sqlparser.IntVal:
		buf.WriteArg(fmt.Sprintf(":v%d", bindIndex))
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
