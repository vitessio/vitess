// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "code.google.com/p/vitess/go/vt/client2/tablet"
)

var usage = `
The parameters are first the SQL command, then the bound variables.
For query arguments, we assume place-holders in the query string
in the form of :v0, :v1, etc.
`

var count = flag.Int("count", 1, "how many times to run the query")
var dml = flag.Bool("dml", false, "this is a dml query, use a transaction")
var server = flag.String("server", "localhost:6603/test", "vtocc server as hostname:port/dbname or user:password@hostname:port/dbname")
var streaming = flag.Bool("streaming", false, "use the streaming API")
var verbose = flag.Bool("verbose", false, "show results")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)

	}
}

func main() {
	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	var driver string

	if *streaming {
		driver = "vttablet-streaming"
	} else {
		driver = "vttablet"
	}

	db, err := sql.Open(driver, *server)
	if err != nil {
		log.Fatalf("client error: %v", err)
	}

	log.Println("Sending the query...")
	now := time.Now()

	// handle dml
	if *dml {
		t, err := db.Begin()
		if err != nil {
			log.Fatalf("begin failed: %v", err)
		}

		r, err := t.Exec(args[0])
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
		r, err := db.Query(args[0])
		if err != nil {
			log.Fatalf("client error: %v", err)
		}

		// get the headers
		cols, err := r.Columns()
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
		for r.Next() {
			row := make([]sql.NullString, len(cols))
			rowi := make([]interface{}, len(cols))
			for i := 0; i < len(cols); i++ {
				rowi[i] = &row[i]
			}
			err := r.Scan(rowi...)
			if err != nil {
				log.Fatalf("Error %s\n", err.Error())
			}

			// print the line if needed
			if *verbose {
				line := fmt.Sprintf("%d", rowIndex)
				for _, value := range row {
					if value.Valid {
						line += value.String + "\t"
					} else {
						line += "\t"
					}
				}
				log.Println(line)
			}
			rowIndex++
		}
		log.Println("Total time:", time.Now().Sub(now), "Row count:", rowIndex)
	}
}
