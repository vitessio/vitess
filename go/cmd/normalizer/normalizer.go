// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// normalizer takes a file of sql statements as input and converts the
// statements into normalized sql statements with bind variables.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/vt/sqlparser"
)

type NormalizedQuery struct {
	Sql      []byte
	BindVars map[string]interface{}
	Line     int
}

func main() {
	infile := flag.String("input", "", "input file name")
	outfile := flag.String("output", "", "output file name (bson)")
	flag.Parse()
	outfd, err := os.Create(*outfile)
	if err != nil {
		panic(fmt.Sprintf("Could not open file %s", *outfile))
	}
	defer outfd.Close()
	lineno := 0
	skipLines := false
	for sql := range iterateFile(*infile) {
		lineno++
		// skip admin user actions
		lstr := strings.ToLower(sql)
		if strings.HasPrefix(lstr, "use") {
			if strings.HasPrefix(lstr, "use admin") {
				skipLines = true
			} else {
				skipLines = false
			}
		}
		if skipLines {
			continue
		}
		if newsql, bvars, err := Normalize(sql); err != nil {
			fmt.Printf("Line %d: Error: %v\n", lineno, err)
		} else {
			if newsql == "" {
				continue
			}
			nq := &NormalizedQuery{[]byte(newsql), bvars, lineno}
			if data, err := bson.Marshal(nq); err != nil {
				fmt.Printf("Line %d: Error: %v\n", lineno, err)
			} else {
				outfd.Write(data)
			}
		}
	}
}

func Normalize(sql string) (string, map[string]interface{}, error) {
	if sql == "" || sql[0] == '#' || sql[0] == '/' {
		return "", nil, nil
	} else {
		lstr := strings.ToLower(sql)
		if strings.HasPrefix(lstr, "begin") {
			return "begin", nil, nil
		}
		if strings.HasPrefix(lstr, "commit") {
			return "commit", nil, nil
		}
		if strings.HasPrefix(lstr, "rollback") {
			return "rollback", nil, nil
		}
		if strings.HasPrefix(lstr, "use") {
			return "", nil, nil
		}
	}
	tree, err := sqlparser.Parse(sql)
	if err != nil {
		return "", nil, err
	}
	return NormalizeTree(tree)
}

func NormalizeTree(tree *sqlparser.Node) (normalized string, bindVars map[string]interface{}, err error) {
	defer handleError(&err)

	bindVars = make(map[string]interface{})
	counter := 0
	NormalizeNode(tree, bindVars, &counter)
	return tree.String(), bindVars, nil
}

func NormalizeNode(node *sqlparser.Node, bindVars map[string]interface{}, counter *int) {
	for i := 0; i < node.Len(); i++ {
		switch node.At(i).Type {
		case sqlparser.STRING:
			*counter++
			bindVars[fmt.Sprintf("v%d", *counter)] = string(node.At(i).Value)
			node.Set(i, newArgumentNode(counter))
		case sqlparser.NUMBER:
			*counter++
			varname := fmt.Sprintf("v%d", *counter)
			valstr := string(node.At(i).Value)
			if ival, err := strconv.ParseInt(valstr, 0, 64); err == nil {
				bindVars[varname] = ival
			} else if uval, err := strconv.ParseUint(valstr, 0, 64); err == nil {
				bindVars[varname] = uval
			} else if fval, err := strconv.ParseFloat(valstr, 64); err == nil {
				bindVars[varname] = fval
			} else {
				panic(sqlparser.NewParserError("%v", err))
			}
			node.Set(i, newArgumentNode(counter))
		default:
			for i := 0; i < node.Len(); i++ {
				NormalizeNode(node.At(i), bindVars, counter)
			}
		}
	}
}

func newArgumentNode(counter *int) *sqlparser.Node {
	return sqlparser.NewSimpleParseNode(sqlparser.VALUE_ARG, fmt.Sprintf("%%(v%d)s", *counter))
}

func iterateFile(name string) (sqls chan string) {
	fd, err := os.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		panic(fmt.Sprintf("Could not open file %s", name))
	}
	sqls = make(chan string)
	go func() {
		defer close(sqls)

		r := bufio.NewReader(fd)
		lineno := 0
		for {
			line, err := r.ReadString('\n')
			line = strings.TrimRight(line, ";\n")
			line = strings.Replace(line, "/*!*/", "", -1)
			lineno++
			if err != nil {
				if err != io.EOF {
					panic(fmt.Sprintf("Error reading file %s: %s", name, err.Error()))
				}
				break
			}
			sqls <- line
		}
	}()
	return sqls
}

func handleError(err *error) {
	if x := recover(); x != nil {
		*err = x.(sqlparser.ParserError)
	}
}
