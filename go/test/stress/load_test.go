/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stress

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	keyspaceName    = "ks"
	cell            = "zone1"
	sqlSchema       = `
		create table main (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false)
		if err != nil {
			return 1
		}

		// Start vtgate
		err = clusterInstance.StartVtgate()
		if err != nil {
			return 1
		}

		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestSimpleStressTest(t *testing.T) {
	defer cluster.PanicHandler(t)
	insertInitialTable(t)

	fmt.Println("Starting load testing ...")

	clientLimit := 2
	duration := 1 * time.Second

	done := make(chan bool, clientLimit)

	for i := 0; i < clientLimit; i++ {
		go startStressClient(t, duration, done)
	}

	for i := 0; i < clientLimit; i++ {
		<-done
	}
}

func insertInitialTable(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// TODO: move to `insert` case
	exec(t, conn, `insert into main(id, val) values(0,'test'),(1,'value')`)
}

func startStressClient(t *testing.T, duration time.Duration, done chan bool) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	var selectCount int

	timeout := time.After(duration)
	for {
		select {
		case <-timeout:
			fmt.Println("QPS:", selectCount/int(duration.Seconds()))
			done <- true
			return
		case <-time.After(1 * time.Microsecond): // selects
			assertMatches(t, conn, `select id from main`, `[[INT64(0)] [INT64(1)]]`)
			selectCount++
		}
	}
}

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}
