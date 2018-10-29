/*
Copyright 2018 The Vitess Authors

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

package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vtbench"

	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

/*

  Vtbench is a simple load testing client to compare workloads in
  Vitess across the various client/server protocols.

  There are a number of command line options to control the behavior,
  but as a basic example, the three supported client protocols are:

  Mysql protocol to vtgate:
  vtbench \
        -protocol mysql \
        -host vtgate-host.my.domain \
        -port 15306 \
        -user db_username \
        -db-credentials-file ./vtbench_db_creds.json \
        -db @replica \
        -sql "select * from loadtest_table where id=123456789" \
        -threads 10 \
        -count 10

  GRPC to vtgate:
  vtbench \
        -protocol grpc-vtgate \
        -host vtgate-host.my.domain \
        -port 15999 \
        -db @replica  \
        $VTTABLET_GRPC_ARGS \
        -sql "select * from loadtest_table where id=123456789" \
        -threads 10 \
        -count 10

  GRPC to vttablet:
  vtbench \
        -protocol grpc-vttablet \
        -host tablet-loadtest-00-80.my.domain \
        -port 15999 \
        -db loadtest/00-80@replica  \
        -sql "select * from loadtest_table where id=123456789" \
        -threads 10 \
        -count 10

*/

var (
	// connection flags
	host       = flag.String("host", "", "vtgate host(s) in the form 'host1,host2,...'")
	port       = flag.Int("port", 0, "vtgate port")
	unixSocket = flag.String("unix_socket", "", "vtgate unix socket")
	protocol   = flag.String("protocol", "mysql", "client protocol, either mysql (default), grpc-vtgate, or grpc-vttablet")
	user       = flag.String("user", "", "username to connect using mysql (password comes from the db-credentials-file)")
	db         = flag.String("db", "", "db name to use when connecting / running the queries (e.g. @replica, keyspace, keyspace/shard etc)")

	// test flags
	deadline = flag.Duration("deadline", 5*time.Minute, "maximum duration for the test run (default 5 minutes)")
	sql      = flag.String("sql", "", "sql statement to execute")
	threads  = flag.Int("threads", 2, "number of parallel threads to run")
	count    = flag.Int("count", 1000, "number of queries per thread")
)

func main() {
	logger := logutil.NewConsoleLogger()
	flag.CommandLine.SetOutput(logutil.NewLoggerWriter(logger))

	defer exit.Recover()

	flag.Lookup("logtostderr").Value.Set("true")
	flag.Parse()

	clientProto := vtbench.MySQL
	switch *protocol {
	case "", "mysql":
		clientProto = vtbench.MySQL
	case "grpc-vtgate":
		clientProto = vtbench.GRPCVtgate
	case "grpc-vttablet":
		clientProto = vtbench.GRPCVttablet
	default:
		log.Exitf("invalid client protocol %s", *protocol)
	}

	if (*host != "" || *port != 0) && *unixSocket != "" {
		log.Exitf("can't specify both host:port and unix_socket")
	}

	if *host != "" && *port == 0 {
		log.Exitf("must specify port when using host")
	}

	if *host == "" && *port != 0 {
		log.Exitf("must specify host when using port")
	}

	if *host == "" && *port == 0 && *unixSocket == "" {
		log.Exitf("vtbench requires either host/port or unix_socket")
	}

	if *sql == "" {
		log.Exitf("must specify sql")
	}

	var password string
	if clientProto == vtbench.MySQL {
		var err error
		_, password, err = dbconfigs.GetCredentialsServer().GetUserAndPassword(*user)
		if err != nil {
			log.Exitf("error reading password for user %v from file: %v", *user, err)
		}
	}

	connParams := vtbench.ConnParams{
		Hosts:      strings.Split(*host, ","),
		Port:       *port,
		UnixSocket: *unixSocket,
		Protocol:   clientProto,
		DB:         *db,
		Username:   *user,
		Password:   password,
	}

	b := vtbench.NewBench(*threads, *count, connParams, *sql)

	ctx, cancel := context.WithTimeout(context.Background(), *deadline)
	defer cancel()

	fmt.Printf("Initializing test with %s protocol / %d threads / %d iterations\n",
		b.ConnParams.Protocol.String(), b.Threads, b.Count)
	err := b.Run(ctx)
	if err != nil {
		log.Exitf("error in test: %v", err)
	}

	fmt.Printf("Average Rows Returned: %d\n", b.Rows.Get()/int64(b.Threads*b.Count))
	fmt.Printf("Average Query Time: %v\n", time.Duration(b.Timings.Time()/b.Timings.Count()))
	fmt.Printf("Total Test Time: %v\n", b.TotalTime)
	fmt.Printf("QPS (Per Thread): %v\n", float64(b.Count)/b.TotalTime.Seconds())
	fmt.Printf("QPS (Total): %v\n", float64(b.Count*b.Threads)/b.TotalTime.Seconds())

	last := int64(0)

	histograms := b.Timings.Histograms()
	h := histograms["query"]
	buckets := h.Buckets()
	fmt.Printf("Query Timings:\n")
	for i, bucket := range h.Cutoffs() {
		count := buckets[i]
		if count != 0 {
			fmt.Printf("%v-%v: %v\n", time.Duration(last), time.Duration(bucket), count)
		}
		last = bucket
	}
}
