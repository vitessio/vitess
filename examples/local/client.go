// client.go is a sample for using the Go Vitess client with an unsharded keyspace.
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
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	// import the 'vitess' sql driver
	_ "github.com/youtube/vitess/go/vt/client"
)

var (
	server = flag.String("server", "", "vtgate server to connect to")
)

func main() {
	keyspace := "test_keyspace"
	timeout := (10 * time.Second).Nanoseconds()
	shard := "0"

	flag.Parse()

	// Connect to vtgate.
	connStr := fmt.Sprintf(`{"protocol": "grpc", "address": "%s", "keyspace": "%s", "shard": "%s", "tablet_type": "%s", "streaming": %v, "timeout": %d}`,
		*server, keyspace, shard, "master", false, timeout)
	db, err := sql.Open("vitess", connStr)
	if err != nil {
		fmt.Printf("client error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Insert something.
	fmt.Println("Inserting into master...")
	tx, err := db.Begin()
	if err != nil {
		fmt.Printf("begin failed: %v\n", err)
		os.Exit(1)
	}
	if _, err := tx.Exec("INSERT INTO test_table (msg) VALUES (?)", "V is for speed"); err != nil {
		fmt.Printf("exec failed: %v\n", err)
		os.Exit(1)
	}
	if err := tx.Commit(); err != nil {
		fmt.Printf("commit failed: %v\n", err)
		os.Exit(1)
	}

	// Read it back from the master.
	fmt.Println("Reading from master...")
	rows, err := db.Query("SELECT id, msg FROM test_table")
	if err != nil {
		fmt.Printf("query failed: %v\n", err)
		os.Exit(1)
	}
	for rows.Next() {
		var id int
		var msg string
		if err := rows.Scan(&id, &msg); err != nil {
			fmt.Printf("scan failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("(%v, %q)\n", id, msg)
	}
	if err := rows.Err(); err != nil {
		fmt.Printf("row iteration failed: %v\n", err)
		os.Exit(1)
	}

	// Read from a replica.
	// Note that this may be behind master due to replication lag.
	fmt.Println("Reading from replica...")

	connStr = fmt.Sprintf(`{"protocol": "grpc", "address": "%s", "keyspace": "%s", "shard": "%s", "tablet_type": "%s", "streaming": %v, "timeout": %d}`,
		*server, keyspace, shard, "replica", false, timeout)
	dbr, err := sql.Open("vitess", connStr)
	if err != nil {
		fmt.Printf("client error: %v\n", err)
		os.Exit(1)
	}
	defer dbr.Close()

	rows, err = dbr.Query("SELECT id, msg FROM test_table")
	if err != nil {
		fmt.Printf("query failed: %v\n", err)
		os.Exit(1)
	}
	for rows.Next() {
		var id int
		var msg string
		if err := rows.Scan(&id, &msg); err != nil {
			fmt.Printf("scan failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("(%v, %q)\n", id, msg)
	}
	if err := rows.Err(); err != nil {
		fmt.Printf("row iteration failed: %v\n", err)
		os.Exit(1)
	}
}
