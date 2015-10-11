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

		err = framework.StartDefaultServer(connParams, schemaOverrides)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v", err)
			return 1
		}
		defer framework.StopDefaultServer()

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
	tableacl.Init(file.Name())
	return nil
}

var testSchema = `create table vtocc_test(intval int, floatval float, charval varchar(256), binval varbinary(256), primary key(intval)) comment 'vtocc_nocache';
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
      "name": "vtocc_cached",
      "table_names_or_prefixes": ["vtocc_nocache", "vtocc_cached%"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vtocc_renamed",
      "table_names_or_prefixes": ["vtocc_renamed%"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vtocc_part",
      "table_names_or_prefixes": ["vtocc_part%"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vtocc",
      "table_names_or_prefixes": ["vtocc_a", "vtocc_b", "vtocc_c", "dual", "vtocc_d", "vtocc_temp", "vtocc_e", "vtocc_f", "upsert_test", "vtocc_strings", "vtocc_fracts", "vtocc_ints", "vtocc_misc", "vtocc_big", "vtocc_view"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vtocc_test",
      "table_names_or_prefixes": ["vtocc_test"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vtocc_acl_unmatched",
      "table_names_or_prefixes": ["vtocc_acl_unmatched"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vtocc_acl_no_access",
      "table_names_or_prefixes": ["vtocc_acl_no_access"]
    },
    {
      "name": "vtocc_acl_read_only",
      "table_names_or_prefixes": ["vtocc_acl_read_only"],
      "readers": ["dev"]
    },
    {
      "name": "vtocc_acl_read_write",
      "table_names_or_prefixes": ["vtocc_acl_read_write"],
      "readers": ["dev"],
      "writers": ["dev"]
    },
    {
      "name": "vtocc_acl_admin",
      "table_names_or_prefixes": ["vtocc_acl_admin"],
      "readers": ["dev"],
      "writers": ["dev"],
      "admins": ["dev"]
    },
    {
      "name": "vtocc_acl_all_user_read_only",
      "table_names_or_prefixes": ["vtocc_acl_all_user_read_only"],
      "readers": ["dev"]
    }
  ]
}`

var schemaOverrideJSON = `[{
	"Name": "vtocc_view",
	"PKColumns": ["key2"],
	"Cache": {
		"Type": "RW"
	}
}, {
	"Name": "vtocc_part1",
	"PKColumns": ["key2"],
	"Cache": {
		"Type": "W",
		"Table": "vtocc_view"
	}
}, {
	"Name": "vtocc_part2",
	"PKColumns": ["key3"],
	"Cache": {
		"Type": "W",
		"Table": "vtocc_view"
	}
}]`
