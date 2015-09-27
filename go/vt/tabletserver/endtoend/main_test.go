// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vttest"

	// import mysql to register mysql connection function

	// import memcache to register memcache connection function
	_ "github.com/youtube/vitess/go/memcache"
)

var (
	connParams    sqldb.ConnParams
	baseConfig    tabletserver.Config
	target        query.Target
	defaultServer *tabletserver.TabletServer
	serverAddress string
)

func TestMain(m *testing.M) {
	flag.Parse()
	tabletserver.Init()

	exitCode := func() int {
		hdl, err := vttest.LauncMySQL("vttest", schema, testing.Verbose())
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not launch mysql: %v\n", err)
			return 1
		}
		defer hdl.TearDown()
		connParams, err = hdl.MySQLConnParams()
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not fetch mysql params: %v\n", err)
			return 1
		}
		dbcfgs := dbconfigs.DBConfigs{
			App: dbconfigs.DBConfig{
				ConnParams:        connParams,
				Keyspace:          "vttest",
				Shard:             "0",
				EnableRowcache:    true,
				EnableInvalidator: false,
			},
		}

		mysqld := mysqlctl.NewMysqld(
			"Dba",
			"App",
			&mysqlctl.Mycnf{},
			&dbcfgs.Dba,
			&dbcfgs.App.ConnParams,
			&dbcfgs.Repl)

		baseConfig = tabletserver.DefaultQsConfig
		baseConfig.RowCache.Binary = vttest.MemcachedPath()
		baseConfig.RowCache.Socket = path.Join(os.TempDir(), "memcache.sock")
		baseConfig.EnableAutoCommit = true

		target = query.Target{
			Keyspace:   "vttest",
			Shard:      "0",
			TabletType: topodata.TabletType_MASTER,
		}

		defaultServer = tabletserver.NewTabletServer(baseConfig)
		defaultServer.Register()
		err = defaultServer.StartService(&target, &dbcfgs, nil, mysqld)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not start service: %v\n", err)
			return 1
		}
		defer defaultServer.StopService()

		// Start http service.
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not start listener: %v\n", err)
			return 1
		}
		serverAddress = fmt.Sprintf("http://%s", ln.Addr().String())
		go http.Serve(ln, nil)
		for {
			time.Sleep(10 * time.Millisecond)
			response, err := http.Get(fmt.Sprintf("%s/debug/vars", serverAddress))
			if err == nil {
				response.Body.Close()
				break
			}
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

// debugVars parses /debug/vars and returns a map. The function returns
// an empty map on error.
func debugVars() map[string]interface{} {
	out := map[string]interface{}{}
	response, err := http.Get(fmt.Sprintf("%s/debug/vars", serverAddress))
	if err != nil {
		return out
	}
	defer response.Body.Close()
	_ = json.NewDecoder(response.Body).Decode(&out)
	return out
}

// fetchInt fetches the specified dot-separated tag and returns the
// value as an int. It returns 0 on error, or if not found.
func fetchInt(vars map[string]interface{}, tags string) int {
	val, _ := fetchVal(vars, tags).(float64)
	return int(val)
}

// fetchVal fetches the specified dot-separated tag and returns the
// value as an interface. It returns nil on error, or if not found.
func fetchVal(vars map[string]interface{}, tags string) interface{} {
	splitTags := strings.Split(tags, ".")
	if len(tags) == 0 {
		return nil
	}
	current := vars
	for _, tag := range splitTags[:len(splitTags)-1] {
		icur, ok := current[tag]
		if !ok {
			return nil
		}
		current, ok = icur.(map[string]interface{})
		if !ok {
			return nil
		}
	}
	return current[splitTags[len(splitTags)-1]]
}

// newServer starts a new unregistered tabletserver.
// You have to end it with StopService once testing is done.
func newServer(config tabletserver.Config, enableRowcache bool) (*tabletserver.TabletServer, error) {
	dbcfgs := dbconfigs.DBConfigs{
		App: dbconfigs.DBConfig{
			ConnParams:        connParams,
			Keyspace:          "vttest",
			Shard:             "0",
			EnableRowcache:    enableRowcache,
			EnableInvalidator: false,
		},
	}

	mysqld := mysqlctl.NewMysqld(
		"Dba",
		"App",
		&mysqlctl.Mycnf{},
		&dbcfgs.Dba,
		&dbcfgs.App.ConnParams,
		&dbcfgs.Repl)

	target = query.Target{
		Keyspace:   "vttest",
		Shard:      "0",
		TabletType: topodata.TabletType_MASTER,
	}

	server := tabletserver.NewTabletServer(config)
	err := server.StartService(&target, &dbcfgs, nil, mysqld)
	if err != nil {
		return nil, err
	}
	return server, nil
}

// queryClient provides a convenient wrapper for TabletServer's query service.
// It's not thread safe, but you can create multiple clients that point to the
// same server.
type queryClient struct {
	target        query.Target
	server        *tabletserver.TabletServer
	transactionID int64
}

// newClient creates a new client for the specified server.
func newClient(server *tabletserver.TabletServer) *queryClient {
	return &queryClient{
		target: target,
		server: server,
	}
}

func (client *queryClient) Begin() error {
	if client.transactionID != 0 {
		return errors.New("already in transaction")
	}
	var txinfo proto.TransactionInfo
	err := client.server.Begin(context.Background(), &client.target, nil, &txinfo)
	if err != nil {
		return err
	}
	client.transactionID = txinfo.TransactionId
	return nil
}

func (client *queryClient) Commit() error {
	defer func() { client.transactionID = 0 }()
	return client.server.Commit(context.Background(), &client.target, nil)
}

func (client *queryClient) Rollback() error {
	defer func() { client.transactionID = 0 }()
	return client.server.Rollback(context.Background(), &client.target, nil)
}

func (client *queryClient) Execute(query string, bindvars map[string]interface{}) (*mproto.QueryResult, error) {
	var qr = &mproto.QueryResult{}
	err := client.server.Execute(
		context.Background(),
		&client.target,
		&proto.Query{
			Sql:           query,
			BindVariables: bindvars,
			TransactionId: client.transactionID,
		},
		qr,
	)
	return qr, err
}

var schema = `create table vtocc_test(intval int, floatval float, charval varchar(256), binval varbinary(256), primary key(intval)) comment 'vtocc_nocache';
delete from vtocc_test;
insert into vtocc_test values(1, 1.12345, 0xC2A2, 0x00FF), (2, null, '', null), (3, null, null, null);

create table vtocc_a(eid bigint default 1, id int default 1, name varchar(128), foo varbinary(128), primary key(eid, id)) comment 'vtocc_nocache';
create table vtocc_b(eid bigint, id int, primary key(eid, id)) comment 'vtocc_nocache';
create table vtocc_c(eid bigint, name varchar(128), foo varbinary(128), primary key(eid, name)) comment 'vtocc_nocache';
create table vtocc_d(eid bigint, id int) comment 'vtocc_nocache';
create table vtocc_e(eid bigint auto_increment, id int default 1, name varchar(128) default 'name', foo varchar(128), primary key(eid, id, name)) comment 'vtocc_nocache';
create table vtocc_f(vb varbinary(16) default 'ab', id int, primary key(vb)) comment 'vtocc_nocache';
create table upsert_test(id1 int, id2 int, primary key (id1)) comment 'vtocc_nocache';
create unique index id2_idx on upsert_test(id2);
delete from vtocc_a;
delete from vtocc_c;
insert into vtocc_a(eid, id, name, foo) values(1, 1, 'abcd', 'efgh'), (1, 2, 'bcde', 'fghi');
insert into vtocc_b(eid, id) values(1, 1), (1, 2);
insert into vtocc_c(eid, name, foo) values(10, 'abcd', '20'), (11, 'bcde', '30');
delete from upsert_test;

create table vtocc_cached1(eid bigint, name varchar(128), foo varbinary(128), primary key(eid));
create index aname1 on vtocc_cached1(name);
delete from vtocc_cached1;
insert into vtocc_cached1 values (1, 'a', 'abcd');
insert into vtocc_cached1 values (2, 'a', 'abcd');
insert into vtocc_cached1 values (3, 'c', 'abcd');
insert into vtocc_cached1 values (4, 'd', 'abcd');
insert into vtocc_cached1 values (5, 'e', 'efgh');
insert into vtocc_cached1 values (9, 'i', 'ijkl');

create table vtocc_cached2(eid bigint, bid varbinary(16), name varchar(128), foo varbinary(128), primary key(eid, bid));
create index aname2 on vtocc_cached2(eid, name);
delete from vtocc_cached2;
insert into vtocc_cached2 values (1, 'foo', 'abcd1', 'efgh');
insert into vtocc_cached2 values (1, 'bar', 'abcd1', 'efgh');
insert into vtocc_cached2 values (2, 'foo', 'abcd2', 'efgh');
insert into vtocc_cached2 values (2, 'bar', 'abcd2', 'efgh');

create table vtocc_big(id int, string1 varchar(128), string2 varchar(100), string3 char(1), string4 varchar(50), string5 varchar(50), date1 date, string6 varchar(16), string7 varchar(120), bigint1 bigint(20), bigint2 bigint(20), date2 date, integer1 int, tinyint1 tinyint(4), primary key(id)) comment 'vtocc_big';

create table vtocc_ints(tiny tinyint, tinyu tinyint unsigned, small smallint, smallu smallint unsigned, medium mediumint, mediumu mediumint unsigned, normal int, normalu int unsigned, big bigint, bigu bigint unsigned, y year, primary key(tiny)) comment 'vtocc_nocache';
create table vtocc_fracts(id int, deci decimal(5,2), num numeric(5,2), f float, d double, primary key(id)) comment 'vtocc_nocache';
create table vtocc_strings(vb varbinary(16), c char(16), vc varchar(16), b binary(4), tb tinyblob, bl blob, ttx tinytext, tx text, en enum('a','b'), s set('a','b'), primary key(vb)) comment 'vtocc_nocache';
create table vtocc_misc(id int, b bit(8), d date, dt datetime, t time, primary key(id)) comment 'vtocc_nocache';

create table vtocc_part1(key1 bigint, key2 bigint, data1 int, primary key(key1, key2));
create unique index vtocc_key2 on vtocc_part1(key2);
create table vtocc_part2(key3 bigint, data2 int, primary key(key3));
create view vtocc_view as select key2, key1, data1, data2 from vtocc_part1, vtocc_part2 where key2=key3;
delete from vtocc_part1;
delete from vtocc_part2;
insert into vtocc_part1 values(10, 1, 1);
insert into vtocc_part1 values(10, 2, 2);
insert into vtocc_part2 values(1, 3);
insert into vtocc_part2 values(2, 4);

create table vtocc_acl_no_access(key1 bigint, key2 bigint, primary key(key1));
create table vtocc_acl_read_only(key1 bigint, key2 bigint, primary key(key1));
create table vtocc_acl_read_write(key1 bigint, key2 bigint, primary key(key1));
create table vtocc_acl_admin(key1 bigint, key2 bigint, primary key(key1));
create table vtocc_acl_unmatched(key1 bigint, key2 bigint, primary key(key1));
create table vtocc_acl_all_user_read_only(key1 bigint, key2 bigint, primary key(key1));`
