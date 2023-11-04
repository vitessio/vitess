/*
Copyright 2023 The Vitess Authors.

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

package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/grpccommon"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vtbench"

	// Import and register the gRPC vtgateconn client
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	// Import and register the gRPC tabletconn client
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

/*

  Vtbench is a simple load testing client to compare workloads in
  Vitess across the various client/server protocols.

  There are a number of command line options to control the behavior,
  but as a basic example, the three supported client protocols are:

  Mysql protocol to vtgate:
  vtbench \
        --protocol mysql \
        --host vtgate-host.my.domain \
        --port 15306 \
        --user db_username \
        --db-credentials-file ./vtbench_db_creds.json \
        --db @replica \
        --sql "select * from loadtest_table where id=123456789" \
        --threads 10 \
        --count 10

  GRPC to vtgate:
  vtbench \
        --protocol grpc-vtgate \
        --host vtgate-host.my.domain \
        --port 15999 \
        --db @replica  \
        $VTTABLET_GRPC_ARGS \
        --sql "select * from loadtest_table where id=123456789" \
        --threads 10 \
        --count 10

  GRPC to vttablet:
  vtbench \
        --protocol grpc-vttablet \
        --host tablet-loadtest-00-80.my.domain \
        --port 15999 \
        --db loadtest/00-80@replica  \
        --sql "select * from loadtest_table where id=123456789" \
        --threads 10 \
        --count 10

*/

var (
	host, unixSocket, user, db, sql string
	port                            int
	protocol                        = "mysql"
	deadline                        = 5 * time.Minute
	threads                         = 2
	count                           = 1000

	Main = &cobra.Command{
		Use:   "vtbench",
		Short: "vtbench is a simple load testing client to compare workloads in Vitess across the various client/server protocols.",
		Example: `There are a number of command line options to control the behavior,
but as a basic example, the three supported client protocols are:

Mysql protocol to vtgate:
vtbench \
	--protocol mysql \
	--host vtgate-host.my.domain \
	--port 15306 \
	--user db_username \
	--db-credentials-file ./vtbench_db_creds.json \
	--db @replica \
	--sql "select * from loadtest_table where id=123456789" \
	--threads 10 \
	--count 10

GRPC to vtgate:
vtbench \
	--protocol grpc-vtgate \
	--host vtgate-host.my.domain \
	--port 15999 \
	--db @replica  \
	$VTTABLET_GRPC_ARGS \
	--sql "select * from loadtest_table where id=123456789" \
	--threads 10 \
	--count 10

GRPC to vttablet:
vtbench \
	--protocol grpc-vttablet \
	--host tablet-loadtest-00-80.my.domain \
	--port 15999 \
	--db loadtest/00-80@replica  \
	--sql "select * from loadtest_table where id=123456789" \
	--threads 10 \
	--count 10`,
		Args:    cobra.NoArgs,
		Version: servenv.AppVersion.String(),
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}
)

func init() {
	servenv.MoveFlagsToCobraCommand(Main)

	Main.Flags().StringVar(&host, "host", host, "VTGate host(s) in the form 'host1,host2,...'")
	Main.Flags().IntVar(&port, "port", port, "VTGate port")
	Main.Flags().StringVar(&unixSocket, "unix_socket", unixSocket, "VTGate unix socket")
	Main.Flags().StringVar(&protocol, "protocol", protocol, "Client protocol, either mysql (default), grpc-vtgate, or grpc-vttablet")
	Main.Flags().StringVar(&user, "user", user, "Username to connect using mysql (password comes from the db-credentials-file)")
	Main.Flags().StringVar(&db, "db", db, "Database name to use when connecting / running the queries (e.g. @replica, keyspace, keyspace/shard etc)")

	Main.Flags().DurationVar(&deadline, "deadline", deadline, "Maximum duration for the test run (default 5 minutes)")
	Main.Flags().StringVar(&sql, "sql", sql, "SQL statement to execute")
	Main.Flags().IntVar(&threads, "threads", threads, "Number of parallel threads to run")
	Main.Flags().IntVar(&count, "count", count, "Number of queries per thread")

	Main.MarkFlagRequired("sql")

	grpccommon.RegisterFlags(Main.Flags())
	acl.RegisterFlags(Main.Flags())
	servenv.RegisterMySQLServerFlags(Main.Flags())
}

func run(cmd *cobra.Command, args []string) error {
	logger := logutil.NewConsoleLogger()
	cmd.SetOutput(logutil.NewLoggerWriter(logger))
	_ = cmd.Flags().Set("logtostderr", "true")

	servenv.Init()

	var clientProto vtbench.ClientProtocol
	switch protocol {
	case "", "mysql":
		clientProto = vtbench.MySQL
	case "grpc-vtgate":
		clientProto = vtbench.GRPCVtgate
	case "grpc-vttablet":
		clientProto = vtbench.GRPCVttablet
	default:
		return fmt.Errorf("invalid client protocol %s", protocol)
	}

	if (host != "" || port != 0) && unixSocket != "" {
		return errors.New("can't specify both host:port and unix_socket")
	}

	if host != "" && port == 0 {
		return errors.New("must specify port when using host")
	}

	if host == "" && port != 0 {
		return errors.New("must specify host when using port")
	}

	if host == "" && port == 0 && unixSocket == "" {
		return errors.New("vtbench requires either host/port or unix_socket")
	}

	var password string
	if clientProto == vtbench.MySQL {
		var err error
		_, password, err = dbconfigs.GetCredentialsServer().GetUserAndPassword(user)
		if err != nil {
			return fmt.Errorf("error reading password for user %v from file: %w", user, err)
		}
	}

	connParams := vtbench.ConnParams{
		Hosts:      strings.Split(host, ","),
		Port:       port,
		UnixSocket: unixSocket,
		Protocol:   clientProto,
		DB:         db,
		Username:   user,
		Password:   password,
	}

	b := vtbench.NewBench(threads, count, connParams, sql)

	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	fmt.Printf("Initializing test with %s protocol / %d threads / %d iterations\n",
		b.ConnParams.Protocol.String(), b.Threads, b.Count)
	err := b.Run(ctx)
	if err != nil {
		return fmt.Errorf("error in test: %w", err)
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

	return nil
}
