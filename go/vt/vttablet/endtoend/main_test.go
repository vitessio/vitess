// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package endtoend

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/simpleacl"
	"github.com/youtube/vitess/go/vt/vttablet/endtoend/framework"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vttest"
)

var (
	connParams sqldb.ConnParams
)

func TestMain(m *testing.M) {
	flag.Parse() // Do not remove this comment, import into google3 depends on it
	tabletenv.Init()

	exitCode := func() int {
		hdl, err := vttest.LaunchVitess(vttest.MySQLOnly("vttest"), vttest.Schema(testSchema), vttest.Verbose(testing.Verbose()))
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

		err = framework.StartServer(connParams)
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

var testSchema = `create table vitess_test(intval int default 0, floatval float default null, charval varchar(256) default null, binval varbinary(256) default null, primary key(intval));
insert into vitess_test values(1, 1.12345, 0xC2A2, 0x00FF), (2, null, '', null), (3, null, null, null);

create table vitess_a(eid bigint default 0, id int default 1, name varchar(128) default null, foo varbinary(128) default null, primary key(eid, id));
create table vitess_b(eid bigint default 0, id int default 0, primary key(eid, id));
create table vitess_c(eid bigint default 0, name varchar(128) default 'name', foo varbinary(128) default null, primary key(eid, name));
create table vitess_d(eid bigint default null, id int default null);
create table vitess_e(eid bigint auto_increment, id int default 1, name varchar(128) default 'name', foo varchar(128) default null, primary key(eid, id, name));
create table vitess_f(vb varbinary(16) default 'ab', id int default null, primary key(vb));
create table upsert_test(id1 int default 0, id2 int default null, primary key (id1));
create unique index id2_idx on upsert_test(id2);
insert into vitess_a(eid, id, name, foo) values(1, 1, 'abcd', 'efgh'), (1, 2, 'bcde', 'fghi');
insert into vitess_b(eid, id) values(1, 1), (1, 2);
insert into vitess_c(eid, name, foo) values(10, 'abcd', '20'), (11, 'bcde', '30');
create table vitess_mixed_case(Col1 int default 0, COL2 int default null, primary key(col1));

create table vitess_big(id int default 0, string1 varchar(128) default null, string2 varchar(100) default null, string3 char(1) default null, string4 varchar(50) default null, string5 varchar(50) default null, string6 varchar(16) default null, string7 varchar(120) default null, bigint1 bigint(20) default null, bigint2 bigint(20) default null, integer1 int default null, tinyint1 tinyint(4) default null, primary key(id));

create table vitess_ints(tiny tinyint default 0, tinyu tinyint unsigned default null, small smallint default null, smallu smallint unsigned default null, medium mediumint default null, mediumu mediumint unsigned default null, normal int default null, normalu int unsigned default null, big bigint default null, bigu bigint unsigned default null, y year default null, primary key(tiny));
create table vitess_fracts(id int default 0, deci decimal(5,2) default null, num numeric(5,2) default null, f float default null, d double default null, primary key(id));
create table vitess_strings(vb varbinary(16) default 'vb', c char(16) default null, vc varchar(16) default null, b binary(4) default null, tb tinyblob default null, bl blob default null, ttx tinytext default null, tx text default null, en enum('a','b') default null, s set('a','b') default null, primary key(vb));
create table vitess_misc(id int default 0, b bit(8) default null, d date default null, dt datetime default null, t time default null, g geometry default null, primary key(id));
create table vitess_unsupported(id int default 0, pt point default null, primary key(id));

create table vitess_bool(auto int auto_increment, bval tinyint(1) default 0, sval varchar(16) default '', ival int default null, primary key (auto));

create table vitess_seq(id int default 0, next_id bigint default null, cache bigint default null, increment bigint default null, primary key(id)) comment 'vitess_sequence';
insert into vitess_seq(id, next_id, cache) values(0, 1, 3);

create table vitess_acl_no_access(key1 bigint default 0, key2 bigint default null, primary key(key1));
create table vitess_acl_read_only(key1 bigint default 0, key2 bigint default null, primary key(key1));
create table vitess_acl_read_write(key1 bigint default 0, key2 bigint default null, primary key(key1));
create table vitess_acl_admin(key1 bigint default 0, key2 bigint default null, primary key(key1));
create table vitess_acl_unmatched(key1 bigint default 0, key2 bigint default null, primary key(key1));
create table vitess_acl_all_user_read_only(key1 bigint default 0, key2 bigint default null, primary key(key1));`

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
      "table_names_or_prefixes": ["vitess_a", "vitess_b", "vitess_c", "dual", "vitess_d", "vitess_temp", "vitess_e", "vitess_f", "vitess_mixed_case", "upsert_test", "vitess_strings", "vitess_fracts", "vitess_ints", "vitess_misc", "vitess_big", "vitess_view", "vitess_json", "vitess_bool"],
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
      "name": "vitess_message",
      "table_names_or_prefixes": ["vitess_message"],
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
