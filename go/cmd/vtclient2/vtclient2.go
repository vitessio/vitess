// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"code.google.com/p/vitess/go/db"
	_ "code.google.com/p/vitess/go/vt/client2"
	_ "code.google.com/p/vitess/go/vt/client2/tablet"
)

var usage = `
The parameters are first the SQL command, then the bound variables.
For query arguments, we assume place-holders in the query string
in the form of :v0, :v1, etc.
`

var count = flag.Int("count", 1, "how many times to run the query")
var bindvars = FlagMap("bindvars", "bind vars as a json dictionary")
var server = flag.String("server", "localhost:6603/test_keyspace/0", "vtocc server as [user:password@]hostname:port/keyspace/shard[#keyrangestart-keyrangeend]")
var driver = flag.String("driver", "vttablet", "which driver to use (one of vttablet, vttablet-streaming, vtdb, vtdb-streaming)")
var verbose = flag.Bool("verbose", false, "show results")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)

	}
}

//----------------------------------

type Map map[string]interface{}

func (m *Map) String() string {
	b, err := json.Marshal(*m)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func (m *Map) Set(s string) (err error) {
	err = json.Unmarshal([]byte(s), m)
	if err != nil {
		return err
	}
	// json reads all numbers as float64
	// So, we just ditch floats for bindvars
	for k, v := range *m {
		f, ok := v.(float64)
		if ok {
			if f > 0 {
				(*m)[k] = uint64(f)
			} else {
				(*m)[k] = int64(f)
			}
		}
	}

	return nil
}

func FlagMap(name, usage string) (m map[string]interface{}) {
	m = make(map[string]interface{})
	mm := Map(m)
	flag.Var(&mm, name, usage)
	return m
}

// FIXME(alainjobart) this is a cheap trick. Should probably use the
// query parser if we needed this to be 100% reliable.
func isDml(sql string) bool {
	lower := strings.TrimSpace(strings.ToLower(sql))
	return strings.HasPrefix(lower, "insert") || strings.HasPrefix(lower, "update")
}

func main() {
	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	conn, err := db.Open(*driver, *server)
	if err != nil {
		log.Fatalf("client error: %v", err)
	}

	log.Println("Sending the query...")
	now := time.Now()

	// handle dml
	if isDml(args[0]) {
		t, err := conn.Begin()
		if err != nil {
			log.Fatalf("begin failed: %v", err)
		}

		r, err := conn.Exec(args[0], bindvars)
		if err != nil {
			log.Fatalf("exec failed: %v", err)
		}

		err = t.Commit()
		if err != nil {
			log.Fatalf("commit failed: %v", err)
		}

		n, err := r.RowsAffected()
		log.Println("Total time:", time.Now().Sub(now), "Rows affected:", n)
	} else {

		// launch the query
		r, err := conn.Exec(args[0], bindvars)
		if err != nil {
			log.Fatalf("client error: %v", err)
		}

		// get the headers
		cols := r.Columns()
		if err != nil {
			log.Fatalf("client error: %v", err)
		}

		// print the header
		if *verbose {
			line := "Index"
			for _, field := range cols {
				line += "\t" + field
			}
			log.Println(line)
		}

		// get the rows
		rowIndex := 0
		for row := r.Next(); row != nil; row = r.Next() {
			// print the line if needed
			if *verbose {
				line := fmt.Sprintf("%d", rowIndex)
				for _, value := range row {
					if value != nil {
						switch value.(type) {
						case []byte:
							line += fmt.Sprintf("\t%s", value)
						default:
							line += fmt.Sprintf("\t%v", value)
						}
					} else {
						line += "\t"
					}
				}
				log.Println(line)
			}
			rowIndex++
		}
		if err := r.Err(); err != nil {
			log.Fatalf("Error %v\n", err)
		}
		log.Println("Total time:", time.Now().Sub(now), "Row count:", rowIndex)
	}
}
