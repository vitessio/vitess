/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// client.go is a sample for using the Vitess Go SQL driver.
//
// Before running this, start up a local example cluster as described in the
// README.md file.
//
// Then run:
// vitess$ . dev.env
// vitess$ cd examples/local
// vitess/examples/local$ go run client.go -server=localhost:15991
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/vitessdriver"
)

var (
	server = flag.String("server", "", "vtgate server to connect to")
)

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	// Connect to vtgate.
	db, err := vitessdriver.Open(*server, "@master")
	if err != nil {
		fmt.Printf("client error: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = db.Close()
	}()

	// Insert some messages on random pages.
	fmt.Println("Inserting into master...")
	for i := 0; i < 3; i++ {
		tx, err := db.Begin()
		if err != nil {
			fmt.Printf("begin failed: %v\n", err)
			os.Exit(1)
		}
		page := rand.Intn(100) + 1
		timeCreated := time.Now().UnixNano()
		if _, err := tx.Exec("INSERT INTO messages (page,time_created_ns,message) VALUES (?,?,?)",
			page, timeCreated, "V is for speed"); err != nil {
			fmt.Printf("exec failed: %v\n", err)
			os.Exit(1)
		}
		if err := tx.Commit(); err != nil {
			fmt.Printf("commit failed: %v\n", err)
			os.Exit(1)
		}
	}

	// Read it back from the master.
	fmt.Println("Reading from master...")
	rows, err := db.Query("SELECT page, time_created_ns, message FROM messages")
	if err != nil {
		fmt.Printf("query failed: %v\n", err)
		os.Exit(1)
	}
	for rows.Next() {
		var page, timeCreated uint64
		var msg string
		if err := rows.Scan(&page, &timeCreated, &msg); err != nil {
			fmt.Printf("scan failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("(%v, %v, %q)\n", page, timeCreated, msg)
	}
	if err := rows.Err(); err != nil {
		fmt.Printf("row iteration failed: %v\n", err)
		os.Exit(1)
	}

	// Read from a replica.
	// Note that this may be behind master due to replication lag.
	fmt.Println("Reading from replica...")

	dbr, err := vitessdriver.Open(*server, "@replica")
	if err != nil {
		fmt.Printf("client error: %v\n", err)
		os.Exit(1)
	}
	defer func() { vterrors.LogIfError(dbr.Close()) }()

	rows, err = dbr.Query("SELECT page, time_created_ns, message FROM messages")
	if err != nil {
		fmt.Printf("query failed: %v\n", err)
		os.Exit(1)
	}
	for rows.Next() {
		var page, timeCreated uint64
		var msg string
		if err := rows.Scan(&page, &timeCreated, &msg); err != nil {
			fmt.Printf("scan failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("(%v, %v, %q)\n", page, timeCreated, msg)
	}
	if err := rows.Err(); err != nil {
		fmt.Printf("row iteration failed: %v\n", err)
		os.Exit(1)
	}
}
