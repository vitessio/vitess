/*
Copyright 2026 The Vitess Authors.

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

// Package rawresults runs a tablet server with
// --queryserver-config-raw-result-charset against MySQL. It has its own
// TestMain because the flag changes the connections of the server, and the
// tests restart the server with the flag off to compare.
package rawresults

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"

	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	connParams mysql.ConnParams
	dbcfgs     *dbconfigs.DBConfigs
)

func TestMain(m *testing.M) {
	flag.Parse() // Do not remove this comment, import into google3 depends on it
	tabletenv.Init()

	exitCode := func() int {
		cfg := vttest.Config{
			Topology: &vttestpb.VTTestTopology{
				Keyspaces: []*vttestpb.Keyspace{{
					Name:   "vttest",
					Shards: []*vttestpb.Shard{{Name: "0", DbNameOverride: "vttest"}},
				}},
			},
			OnlyMySQL: true,
			Charset:   "utf8mb4",
		}
		if err := cfg.InitSchemas("vttest", "create table placeholder (id int primary key);", nil); err != nil {
			fmt.Fprintf(os.Stderr, "InitSchemas failed: %v\n", err)
			return 1
		}
		defer os.RemoveAll(cfg.SchemaDir)
		cluster := vttest.LocalCluster{Config: cfg}
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "could not launch mysql: %v\n", err)
			return 1
		}
		defer cluster.TearDown()

		connParams = cluster.MySQLConnParams()
		if err := createTable(connParams); err != nil {
			fmt.Fprintf(os.Stderr, "could not create the table: %v\n", err)
			return 1
		}
		dbcfgs = dbconfigs.NewTestDBConfigs(connParams, cluster.MySQLAppDebugConnParams(), cluster.DbName())

		config := tabletenv.NewDefaultConfig()
		config.RawResultCharset = true
		// One connection per pool, so that the session tests see the
		// connection the previous query left behind.
		config.OltpReadPool.Size = 1
		config.OlapReadPool.Size = 1
		config.TxPool.Size = 4
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := framework.StartCustomServer(ctx, connParams, cluster.MySQLAppDebugConnParams(), cluster.DbName(), config); err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer framework.StopServer()
		return m.Run()
	}()
	os.Exit(exitCode)
}

// createTable creates a table with a column of each kind that MySQL converts
// for character_set_results, and of the kinds that it does not.
func createTable(params mysql.ConnParams) error {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, query := range []string{
		"set names utf8mb4",
		`create table rawtext (
			id int primary key,
			l1 varchar(10) character set latin1 collate latin1_german2_ci,
			l1b varchar(10) character set latin1 collate latin1_bin,
			l1t text character set latin1,
			gbk varchar(10) character set gbk,
			sjis varchar(10) character set sjis,
			cp932 varchar(10) character set cp932,
			ujis varchar(10) character set ujis,
			euckr varchar(10) character set euckr,
			gb18030 varchar(10) character set gb18030,
			big5 varchar(10) character set big5,
			tis varchar(10) character set tis620,
			u3 varchar(10) character set utf8mb3,
			u4b varchar(10) character set utf8mb4 collate utf8mb4_bin,
			u4 text character set utf8mb4 collate utf8mb4_0900_ai_ci,
			uc varchar(10) character set ucs2,
			uct text character set ucs2,
			u16 varchar(10) character set utf16,
			u32 varchar(10) character set utf32,
			c char(5) character set latin1,
			tt tinytext character set gbk,
			mt mediumtext character set latin1,
			lt longtext character set latin1,
			e enum('a', 'Ä') character set latin1,
			s set('a', 'Ä') character set latin1,
			j json,
			bn binary(4),
			vb varbinary(10),
			bl blob,
			n int,
			d decimal(10, 2),
			dt datetime,
			bt bit(8),
			g geometry
		)`,
		`insert into rawtext values
			(1, 'Müller', 'Müller', 'Müller', '中文', '日本', '日本', '日本', '한국', '中文😀', '中文', 'ไทย', 'Ü日', 'Ü😀', 'é', 'Ü日', 'Ü日', 'Ü😀', 'Ü😀', 'Ä', '中', 'ß', 'é', 'Ä', 'a,Ä',
				'{"a": "Müller", "b": "日本"}', x'FC00C3BC', x'C3BC', x'FF', 1, 1.5, '2024-02-29 12:00:00', b'1', ST_GeomFromText('POINT(1 2)')),
			(2, 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'a', 'a', 'a', 'a', 'a', 'a',
				'[1, 2]', x'41', x'41', x'41', 2, 2.5, '2024-03-01 00:00:00', b'0', ST_GeomFromText('POINT(3 4)')),
			(3, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
				null, null, null, null, null, null, null, null, null)`,
	} {
		if _, err := conn.ExecuteFetch(query, 0, false); err != nil {
			return fmt.Errorf("%s: %w", query, err)
		}
	}
	return nil
}
