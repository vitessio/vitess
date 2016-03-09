// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/simpleacl"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/endtoend/framework"
	"github.com/youtube/vitess/go/vt/vttest"

	// import mysql to register mysql connection function

	// import memcache to register memcache connection function
	_ "github.com/youtube/vitess/go/memcache"
)

var (
	connParams sqldb.ConnParams
)

func TestMain(m *testing.M) {
	flag.Parse() // Do not remove this comment, import into google3 depends on it
	tabletserver.Init()

	exitCode := func() int {
		hdl, err := vttest.LaunchMySQL("vttest", testSchema, testing.Verbose())
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

		var schemaOverrides []tabletserver.SchemaOverride
		err = json.Unmarshal([]byte(schemaOverrideJSON), &schemaOverrides)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}

		err = framework.StartServer(connParams, schemaOverrides)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer framework.StopServer()

		err = initTableACL()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func initTableACL() error {
	file, err := ioutil.TempFile("", "tableacl.json")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	n, err := file.WriteString(tableACLConfig)
	if err != nil {
		return err
	}
	if n != len(tableACLConfig) {
		return errors.New("table acl: short write")
	}
	file.Close()
	tableacl.Register("simpleacl", &simpleacl.Factory{})
	tableacl.Init(file.Name(), func() { framework.Server.ClearQueryPlanCache() })
	return nil
}

var testSchema = `create table vitess_test(intval int, floatval float, charval varchar(256), binval varbinary(256), primary key(intval)) comment 'vitess_nocache';
insert into vitess_test values(1, 1.12345, 0xC2A2, 0x00FF), (2, null, '', null), (3, null, null, null);

create table vitess_a(eid bigint default 1, id int default 1, name varchar(128), foo varbinary(128), primary key(eid, id)) comment 'vitess_nocache';
create table vitess_b(eid bigint, id int, primary key(eid, id)) comment 'vitess_nocache';
create table vitess_c(eid bigint, name varchar(128), foo varbinary(128), primary key(eid, name)) comment 'vitess_nocache';
create table vitess_d(eid bigint, id int) comment 'vitess_nocache';
create table vitess_e(eid bigint auto_increment, id int default 1, name varchar(128) default 'name', foo varchar(128), primary key(eid, id, name)) comment 'vitess_nocache';
create table vitess_f(vb varbinary(16) default 'ab', id int, primary key(vb)) comment 'vitess_nocache';
create table upsert_test(id1 int, id2 int, primary key (id1)) comment 'vitess_nocache';
create unique index id2_idx on upsert_test(id2);
insert into vitess_a(eid, id, name, foo) values(1, 1, 'abcd', 'efgh'), (1, 2, 'bcde', 'fghi');
insert into vitess_b(eid, id) values(1, 1), (1, 2);
insert into vitess_c(eid, name, foo) values(10, 'abcd', '20'), (11, 'bcde', '30');
create table vitess_mixed_case(Col1 int, COL2 int, primary key(col1)) comment 'vitess_nocache';

create table vitess_cached1(eid bigint, name varchar(128), foo varbinary(128), primary key(eid));
create index aname1 on vitess_cached1(name);
insert into vitess_cached1 values (1, 'a', 'abcd');
insert into vitess_cached1 values (2, 'a', 'abcd');
insert into vitess_cached1 values (3, 'c', 'abcd');
insert into vitess_cached1 values (4, 'd', 'abcd');
insert into vitess_cached1 values (5, 'e', 'efgh');
insert into vitess_cached1 values (9, 'i', 'ijkl');

create table vitess_cached2(eid bigint, bid varbinary(16), name varchar(128), foo varbinary(128), primary key(eid, bid));
create index aname2 on vitess_cached2(eid, name);
insert into vitess_cached2 values (1, 'foo', 'abcd1', 'efgh');
insert into vitess_cached2 values (1, 'bar', 'abcd1', 'efgh');
insert into vitess_cached2 values (2, 'foo', 'abcd2', 'efgh');
insert into vitess_cached2 values (2, 'bar', 'abcd2', 'efgh');

create table vitess_big(id int, string1 varchar(128), string2 varchar(100), string3 char(1), string4 varchar(50), string5 varchar(50), string6 varchar(16), string7 varchar(120), bigint1 bigint(20), bigint2 bigint(20), integer1 int, tinyint1 tinyint(4), primary key(id)) comment 'vitess_big';

create table vitess_ints(tiny tinyint, tinyu tinyint unsigned, small smallint, smallu smallint unsigned, medium mediumint, mediumu mediumint unsigned, normal int, normalu int unsigned, big bigint, bigu bigint unsigned, y year, primary key(tiny)) comment 'vitess_nocache';
create table vitess_fracts(id int, deci decimal(5,2), num numeric(5,2), f float, d double, primary key(id)) comment 'vitess_nocache';
create table vitess_strings(vb varbinary(16), c char(16), vc varchar(16), b binary(4), tb tinyblob, bl blob, ttx tinytext, tx text, en enum('a','b'), s set('a','b'), primary key(vb)) comment 'vitess_nocache';
create table vitess_misc(id int, b bit(8), d date, dt datetime, t time, primary key(id)) comment 'vitess_nocache';

create table vitess_part1(key1 bigint, key2 bigint, data1 int, primary key(key1, key2));
create unique index vitess_key2 on vitess_part1(key2);
create table vitess_part2(key3 bigint, data2 int, primary key(key3));
create view vitess_view as select key2, key1, data1, data2 from vitess_part1, vitess_part2 where key2=key3;
insert into vitess_part1 values(10, 1, 1);
insert into vitess_part1 values(10, 2, 2);
insert into vitess_part2 values(1, 3);
insert into vitess_part2 values(2, 4);

create table vitess_seq(id int, next_id bigint, cache bigint, increment bigint, primary key(id)) comment 'vitess_sequence';
insert into vitess_seq values(0, 1, 3, 2);

create table vitess_acl_no_access(key1 bigint, key2 bigint, primary key(key1));
create table vitess_acl_read_only(key1 bigint, key2 bigint, primary key(key1));
create table vitess_acl_read_write(key1 bigint, key2 bigint, primary key(key1));
create table vitess_acl_admin(key1 bigint, key2 bigint, primary key(key1));
create table vitess_acl_unmatched(key1 bigint, key2 bigint, primary key(key1));
create table vitess_acl_all_user_read_only(key1 bigint, key2 bigint, primary key(key1));`

var tableACLConfig = `{
  "table_groups": [
    {
      "name": "mysql",
      "table_names_or_prefixes": [""],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vitess_cached",
      "table_names_or_prefixes": ["vitess_nocache", "vitess_cached%"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vitess_renamed",
      "table_names_or_prefixes": ["vitess_renamed%"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vitess_part",
      "table_names_or_prefixes": ["vitess_part%"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vitess",
      "table_names_or_prefixes": ["vitess_a", "vitess_b", "vitess_c", "dual", "vitess_d", "vitess_temp", "vitess_e", "vitess_f", "vitess_mixed_case", "upsert_test", "vitess_strings", "vitess_fracts", "vitess_ints", "vitess_misc", "vitess_big", "vitess_view"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vitess_test",
      "table_names_or_prefixes": ["vitess_test"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vitess_seq",
      "table_names_or_prefixes": ["vitess_seq"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vitess_acl_unmatched",
      "table_names_or_prefixes": ["vitess_acl_unmatched"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vitess_acl_no_access",
      "table_names_or_prefixes": ["vitess_acl_no_access"]
    },
    {
      "name": "vitess_acl_read_only",
      "table_names_or_prefixes": ["vitess_acl_read_only"],
      "readers": ["dev"]
    },
    {
      "name": "vitess_acl_read_write",
      "table_names_or_prefixes": ["vitess_acl_read_write"],
      "readers": ["dev"],
      "writers": ["dev"]
    },
    {
      "name": "vitess_acl_admin",
      "table_names_or_prefixes": ["vitess_acl_admin"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vitess_acl_all_user_read_only",
      "table_names_or_prefixes": ["vitess_acl_all_user_read_only"],
      "readers": ["dev"]
    }
  ]
}`

var schemaOverrideJSON = `[{
	"Name": "vitess_view",
	"PKColumns": ["key2"],
	"Cache": {
		"Type": "RW"
	}
}, {
	"Name": "vitess_part1",
	"PKColumns": ["key2"],
	"Cache": {
		"Type": "W",
		"Table": "vitess_view"
	}
}, {
	"Name": "vitess_part2",
	"PKColumns": ["key3"],
	"Cache": {
		"Type": "W",
		"Table": "vitess_view"
	}
}]`
