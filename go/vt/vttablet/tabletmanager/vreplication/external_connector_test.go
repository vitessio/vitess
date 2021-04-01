/*
Copyright 2020 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestExternalConnectorCopy(t *testing.T) {
	execStatements(t, []string{
		"create table tab1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.tab1(id int, val varbinary(128), primary key(id))", vrepldb),
		"insert into tab1 values(1, 'a'), (2, 'b')",

		"create table tab2(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.tab2(id int, val varbinary(128), primary key(id))", vrepldb),
		"insert into tab2 values(1, 'a'), (2, 'b')",

		"create table tab3(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.tab3(id int, val varbinary(128), primary key(id))", vrepldb),
		"insert into tab3 values(1, 'a'), (2, 'b')",
	})
	defer execStatements(t, []string{
		"drop table tab1",
		fmt.Sprintf("drop table %s.tab1", vrepldb),
		"drop table tab2",
		fmt.Sprintf("drop table %s.tab2", vrepldb),
		"drop table tab3",
		fmt.Sprintf("drop table %s.tab3", vrepldb),
	})

	filter1 := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "tab1",
			Filter: "",
		}},
	}
	bls1 := &binlogdatapb.BinlogSource{
		ExternalMysql: "exta",
		Filter:        filter1,
	}
	cancel1 := startExternalVReplication(t, bls1, "")

	expectDBClientAndVreplicationQueries(t, []string{
		"begin",
		"insert into tab1(id,val) values (1,'a'), (2,'b')",
		"/update _vt.copy_state",
		"commit",
		"/delete from _vt.copy_state",
		"/update _vt.vreplication set state='Running'",
	}, "")
	execStatements(t, []string{"insert into tab1 values(3, 'c')"})
	expectDBClientQueries(t, []string{
		"begin",
		"insert into tab1(id,val) values (3,'c')",
		"/update _vt.vreplication set pos=",
		"commit",
	})
	// Cancel immediately so we don't deal with spurious updates.
	cancel1()

	// Check that only one connector was created.
	assert.Equal(t, 1, len(playerEngine.ec.connectors))
	filter2 := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "tab2",
			Filter: "",
		}},
	}
	bls2 := &binlogdatapb.BinlogSource{
		ExternalMysql: "exta",
		Filter:        filter2,
	}
	cancel2 := startExternalVReplication(t, bls2, "")

	expectDBClientAndVreplicationQueries(t, []string{
		"begin",
		"insert into tab2(id,val) values (1,'a'), (2,'b')",
		"/update _vt.copy_state",
		"commit",
		"/delete from _vt.copy_state",
		"/update _vt.vreplication set state='Running'",
	}, "")
	cancel2()

	// Check that only one connector was created.
	assert.Equal(t, 1, len(playerEngine.ec.connectors))

	filter3 := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "tab3",
			Filter: "",
		}},
	}
	bls3 := &binlogdatapb.BinlogSource{
		ExternalMysql: "extb",
		Filter:        filter3,
	}
	cancel3 := startExternalVReplication(t, bls3, "")

	expectDBClientAndVreplicationQueries(t, []string{
		"begin",
		"insert into tab3(id,val) values (1,'a'), (2,'b')",
		"/update _vt.copy_state",
		"commit",
		"/delete from _vt.copy_state",
		"/update _vt.vreplication set state='Running'",
	}, "")
	cancel3()

	// Check that there now two connectors.
	assert.Equal(t, 2, len(playerEngine.ec.connectors))
}

func TestExternalConnectorPlay(t *testing.T) {
	execStatements(t, []string{
		"create table tab1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.tab1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table tab1",
		fmt.Sprintf("drop table %s.tab1", vrepldb),
	})

	filter1 := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "tab1",
			Filter: "",
		}},
	}
	bls1 := &binlogdatapb.BinlogSource{
		ExternalMysql: "exta",
		Filter:        filter1,
	}
	pos := masterPosition(t)
	cancel1 := startExternalVReplication(t, bls1, pos)
	defer cancel1()

	execStatements(t, []string{
		"insert into tab1 values(1, 'a'), (2, 'b')",
	})

	expectDBClientAndVreplicationQueries(t, []string{
		"begin",
		"insert into tab1(id,val) values (1,'a')",
		"insert into tab1(id,val) values (2,'b')",
		"/update _vt.vreplication set pos=",
		"commit",
	}, pos)
}

func expectDBClientAndVreplicationQueries(t *testing.T, queries []string, pos string) {
	t.Helper()
	vrepQueries := getExpectedVreplicationQueries(t, pos)
	expectedQueries := append(vrepQueries, queries...)
	expectDBClientQueries(t, expectedQueries)
}

func getExpectedVreplicationQueries(t *testing.T, pos string) []string {
	if pos == "" {
		return []string{
			"/insert into _vt.vreplication",
			"begin",
			"/insert into _vt.copy_state",
			"/update _vt.vreplication set state='Copying'",
			"commit",
			"/update _vt.vreplication set pos=",
		}
	}
	return []string{
		"/insert into _vt.vreplication",
		"/update _vt.vreplication set state='Running'",
	}
}

func startExternalVReplication(t *testing.T, bls *binlogdatapb.BinlogSource, pos string) (cancelr func()) {
	query := binlogplayer.CreateVReplication("test", bls, pos, 9223372036854775807, 9223372036854775807, 0, vrepldb)
	qr, err := playerEngine.Exec(query)
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		query := fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID)
		if _, err := playerEngine.Exec(query); err != nil {
			t.Fatal(err)
		}
		expectDeleteQueries(t)
	}
}
