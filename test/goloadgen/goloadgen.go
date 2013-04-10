// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"code.google.com/p/vitess/go/vt/client2/tablet"
)

func main() {
	goroutines := flag.Int("goroutines", 100, "Number of client goroutines to run")
	connections := flag.Int("connections", 20000, "Number of connections to create")
	queries := flag.Int("queries", 1*1024*1024, "Numberof queries to run")
	flag.Parse()

	perconnections := *connections / *goroutines
	perqueries := *queries / *goroutines
	for i := 0; i < *goroutines; i++ {
		if i != *goroutines-1 {
			go run(perconnections, perqueries)
			*connections -= perconnections
			*queries -= perqueries
		} else {
			go run(*connections, *queries)
		}
	}
	registry.Wait()
}

func run(connections, queries int) {
	registry.Add(1)
	defer registry.Done()

	conns := make([]*tablet.Conn, connections)
	for i := 0; i < connections; i++ {
		var err error
		conns[i], err = tablet.DialTablet(server, false)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return
		}
	}
	time.Sleep(time.Second)
	connid := 0
	count := 0
	bindVars := make(map[string]interface{})
	for {
		for _, plan := range distribution {
			bindVars["id"] = rand.Intn(10000) + 1
			for i := 0; i < plan.iterations; i++ {
				_, err := conns[connid].Exec(baseQuery+plan.whereClause, bindVars)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%v\n", err)
					return
				}
				count++
				connid = (connid + 1) % connections
				if count >= queries {
					return
				}
			}
		}
	}
}

var registry sync.WaitGroup

var server = "localhost:9461/vt_test_keyspace"

var baseQuery = `
select
	id,
	num1,
	char1,
	char2,
	num2,
	char3,
	char4,
	char5,
	num3,
	char6,
	char7,
	date1,
	char8,
	char9,
	num4,
	char10,
	num5,
	num6,
	num7,
	num8,
	char11,
	num9,
	num10,
	num11,
	num12,
	num13,
	num14,
	char12,
	num15,
	num16,
	num17,
	num18,
	num19
from vt_load
`

type RequestPlan struct {
	rowsHint               int
	iterations, variations int
	whereClause            string
}

var distribution = []RequestPlan{
	{1, 63000, 0, " where id = :id"},
	{0, 14000, 0, " where id = 0"},
	{10000, 1, 0, ""},
	{2, 3000, 0, " where id >= :id limit 2"},
	{1000, 1, 0, " where id >= :id limit 1000"},
	{5, 3000, 0, " where id >= :id limit 5"},
	{10, 3000, 0, " where id >= :id limit 10"},
	{20, 6000, 0, " where id >= :id limit 20"},
	{2000, 1, 0, " where id >= :id limit 2000"},
	{50, 7000, 0, " where id >= :id limit 50"},
	{100, 1000, 0, " where id >= :id limit 100"},
	{5000, 1, 0, " limit 5000"},
	{200, 1000, 0, " where id >= :id limit 200"},
	{500, 1000, 0, " where id >= :id limit 500"},
}
