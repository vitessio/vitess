// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/olekukonko/tablewriter"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vitessdriver"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
)

var (
	usage = `
vtclient connects to a vtgate server using the standard go driver API.
Version 3 of the API is used, we do not send any hint to the server.

For query bound variables, we assume place-holders in the query string
in the form of :v1, :v2, etc.
`
	server        = flag.String("server", "", "vtgate server to connect to")
	tabletType    = flag.String("tablet_type", "rdonly", "tablet type to direct queries to")
	timeout       = flag.Duration("timeout", 30*time.Second, "timeout for queries")
	streaming     = flag.Bool("streaming", false, "use a streaming query")
	bindVariables = newBindvars("bind_variables", "bind variables as a json list")
	keyspace      = flag.String("keyspace", "", "Keyspace of a specific keyspace/shard to target. If shard is also specified, disables v3. Otherwise it's the default keyspace to use.")
	jsonOutput    = flag.Bool("json", false, "Output JSON instead of human-readable table")
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)
	}
}

type bindvars []interface{}

func (bv *bindvars) String() string {
	b, err := json.Marshal(bv)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func (bv *bindvars) Set(s string) (err error) {
	err = json.Unmarshal([]byte(s), &bv)
	if err != nil {
		return err
	}
	// json reads all numbers as float64
	// So, we just ditch floats for bindvars
	for i, v := range *bv {
		if f, ok := v.(float64); ok {
			if f > 0 {
				(*bv)[i] = uint64(f)
			} else {
				(*bv)[i] = int64(f)
			}
		}
	}

	return nil
}

// For internal flag compatibility
func (bv *bindvars) Get() interface{} {
	return bv
}

func newBindvars(name, usage string) *bindvars {
	var bv bindvars
	flag.Var(&bv, name, usage)
	return &bv
}

// FIXME(alainjobart) this is a cheap trick. Should probably use the
// query parser if we needed this to be 100% reliable.
func isDml(sql string) bool {
	lower := strings.TrimSpace(strings.ToLower(sql))
	return strings.HasPrefix(lower, "insert") || strings.HasPrefix(lower, "update") || strings.HasPrefix(lower, "delete")
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
		exit.Return(1)
	}

	c := vitessdriver.Configuration{
		Protocol:   *vtgateconn.VtgateProtocol,
		Address:    *server,
		Keyspace:   *keyspace,
		TabletType: *tabletType,
		Timeout:    *timeout,
		Streaming:  *streaming,
	}
	db, err := vitessdriver.OpenWithConfiguration(c)
	if err != nil {
		log.Errorf("client error: %v", err)
		exit.Return(1)
	}

	log.Infof("Sending the query...")
	startTime := time.Now()

	// handle dml
	if isDml(args[0]) {
		tx, err := db.Begin()
		if err != nil {
			log.Errorf("begin failed: %v", err)
			exit.Return(1)
		}

		result, err := db.Exec(args[0], []interface{}(*bindVariables)...)
		if err != nil {
			log.Errorf("exec failed: %v", err)
			exit.Return(1)
		}

		err = tx.Commit()
		if err != nil {
			log.Errorf("commit failed: %v", err)
			exit.Return(1)
		}

		rowsAffected, err := result.RowsAffected()
		lastInsertID, err := result.LastInsertId()
		log.Infof("Total time: %v / Row affected: %v / Last Insert Id: %v", time.Since(startTime), rowsAffected, lastInsertID)
	} else {
		// launch the query
		rows, err := db.Query(args[0], []interface{}(*bindVariables)...)
		if err != nil {
			log.Errorf("client error: %v", err)
			exit.Return(1)
		}
		defer rows.Close()

		// get the headers
		var qr results
		cols, err := rows.Columns()
		if err != nil {
			log.Errorf("client error: %v", err)
			exit.Return(1)
		}
		qr.Fields = cols

		// get the rows
		for rows.Next() {
			row := make([]interface{}, len(cols))
			for i := range row {
				var col string
				row[i] = &col
			}
			if err := rows.Scan(row...); err != nil {
				log.Errorf("client error: %v", err)
				exit.Return(1)
			}

			// unpack []*string into []string
			vals := make([]string, 0, len(row))
			for _, value := range row {
				vals = append(vals, *(value.(*string)))
			}
			qr.Rows = append(qr.Rows, vals)
		}
		if err := rows.Err(); err != nil {
			log.Errorf("Error %v\n", err)
			exit.Return(1)
		}

		if *jsonOutput {
			data, err := json.MarshalIndent(qr, "", "  ")
			if err != nil {
				log.Errorf("cannot marshal data: %v", err)
				exit.Return(1)
			}
			fmt.Print(string(data))
		} else {
			printTable(qr, time.Since(startTime))
		}
	}
}

type results struct {
	Fields []string   `json:"fields"`
	Rows   [][]string `json:"rows"`
}

func printTable(qr results, dur time.Duration) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(qr.Fields)
	table.SetAutoFormatHeaders(false)
	table.AppendBulk(qr.Rows)
	table.Render()
	fmt.Printf("%v rows in set (%v)\n", len(qr.Rows), dur)
}
