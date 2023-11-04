/*
Copyright 2022 The Vitess Authors.

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

package endtoend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var qSelAllRows = "select table_schema, table_name, create_statement from _vt.views"

// Test will validate create view ddls.
func TestCreateViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	ch := make(chan any)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := client.StreamHealthWithContext(ctx, func(shr *querypb.StreamHealthResponse) error {
			views := shr.RealtimeStats.ViewSchemaChanged
			if len(views) != 0 && views[0] == "vitess_view" {
				ch <- true
			}
			return nil
		})
		require.NoError(t, err)
	}()

	defer func() {
		_, err := client.Execute("drop view vitess_view", nil)
		require.NoError(t, err)
		<-ch // wait for views update
	}()

	_, err := client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	<-ch // wait for views update
	// validate the row in _vt.views.
	qr, err := client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Equal(t,
		"[[VARCHAR(\"vttest\") VARCHAR(\"vitess_view\") TEXT(\"CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view` AS select `vitess_a`.`eid` AS `eid`,`vitess_a`.`id` AS `id`,`vitess_a`.`name` AS `name`,`vitess_a`.`foo` AS `foo` from `vitess_a`\")]]",
		fmt.Sprintf("%v", qr.Rows))

	// view already exists. This should fail.
	_, err = client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.ErrorContains(t, err, "'vitess_view' already exists")

	// view already exists, but create or replace syntax should allow it to replace the view.
	_, err = client.Execute("create or replace view vitess_view as select id, foo from vitess_a", nil)
	require.NoError(t, err)

	<-ch // wait for views update
	// validate the row in _vt.views.
	qr, err = client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Equal(t,
		"[[VARCHAR(\"vttest\") VARCHAR(\"vitess_view\") TEXT(\"CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view` AS select `vitess_a`.`id` AS `id`,`vitess_a`.`foo` AS `foo` from `vitess_a`\")]]",
		fmt.Sprintf("%v", qr.Rows))
}

// Test will validate alter view ddls.
func TestAlterViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	ch := make(chan any)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := client.StreamHealthWithContext(ctx, func(shr *querypb.StreamHealthResponse) error {
			views := shr.RealtimeStats.ViewSchemaChanged
			if len(views) != 0 && views[0] == "vitess_view" {
				ch <- true
			}
			return nil
		})
		require.NoError(t, err)
	}()

	defer func() {
		_, err := client.Execute("drop view vitess_view", nil)
		require.NoError(t, err)
		<-ch // wait for views update
	}()

	// view does not exist, should FAIL
	_, err := client.Execute("alter view vitess_view as select * from vitess_a", nil)
	require.ErrorContains(t, err, "Table 'vttest.vitess_view' doesn't exist (errno 1146) (sqlstate 42S02)")

	// create a view.
	_, err = client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	<-ch // wait for views update
	// validate the row in _vt.views.
	qr, err := client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Equal(t,
		"[[VARCHAR(\"vttest\") VARCHAR(\"vitess_view\") TEXT(\"CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view` AS select `vitess_a`.`eid` AS `eid`,`vitess_a`.`id` AS `id`,`vitess_a`.`name` AS `name`,`vitess_a`.`foo` AS `foo` from `vitess_a`\")]]",
		fmt.Sprintf("%v", qr.Rows))

	// view exists, should PASS
	_, err = client.Execute("alter view vitess_view as select id, foo from vitess_a", nil)
	require.NoError(t, err)

	<-ch // wait for views update
	// validate the row in _vt.views.
	qr, err = client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Equal(t,
		"[[VARCHAR(\"vttest\") VARCHAR(\"vitess_view\") TEXT(\"CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view` AS select `vitess_a`.`id` AS `id`,`vitess_a`.`foo` AS `foo` from `vitess_a`\")]]",
		fmt.Sprintf("%v", qr.Rows))
}

// Test will validate drop view ddls.
func TestDropViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	// view does not exist, should FAIL
	_, err := client.Execute("drop view vitess_view", nil)
	require.ErrorContains(t, err, "Unknown table 'vttest.vitess_view'")

	// view does not exist, using if exists clause, should PASS
	_, err = client.Execute("drop view if exists vitess_view", nil)
	require.NoError(t, err)

	// create two views.
	_, err = client.Execute("create view vitess_view1 as select * from vitess_a", nil)
	require.NoError(t, err)
	_, err = client.Execute("create view vitess_view2 as select * from vitess_a", nil)
	require.NoError(t, err)

	// validate both the views are stored in _vt.views.
	waitForResult(t, client, 2, 1*time.Minute)

	// drop vitess_view1, should PASS
	_, err = client.Execute("drop view vitess_view1", nil)
	require.NoError(t, err)

	// drop three views, only vitess_view2 exists.
	// In MySQL 5.7, this would drop vitess_view2, but that behaviour has changed
	// in MySQL 8.0, and not the view isn't dropped. CI is running 8.0, so the remaining test is
	// written with those expectations.
	_, err = client.Execute("drop view vitess_view1, vitess_view2, vitess_view3", nil)
	require.ErrorContains(t, err, "Unknown table 'vttest.vitess_view1,vttest.vitess_view3'")

	// validate ZERO rows in _vt.views.
	waitForResult(t, client, 1, 1*time.Minute)

	// create a view.
	_, err = client.Execute("create view vitess_view1 as select * from vitess_a", nil)
	require.NoError(t, err)

	// drop three views with if exists clause, only vitess_view1 exists. This should PASS but drops the existing view.
	_, err = client.Execute("drop view if exists vitess_view1, vitess_view2, vitess_view3", nil)
	require.NoError(t, err)

	// validate ZERO rows in _vt.views.
	waitForResult(t, client, 0, 1*time.Minute)
}

// TestViewDDLWithInfrSchema will validate information schema queries with views.
func TestViewDDLWithInfrSchema(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute("drop view vitess_view", nil)

	_, err := client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	// show create view.
	qr, err := client.Execute("show create table vitess_view", nil)
	require.NoError(t, err)
	require.Equal(t,
		"[[VARCHAR(\"vitess_view\") VARCHAR(\"CREATE ALGORITHM=UNDEFINED DEFINER=`vt_dba`@`localhost` SQL SECURITY DEFINER VIEW `vitess_view` AS select `vitess_a`.`eid` AS `eid`,`vitess_a`.`id` AS `id`,`vitess_a`.`name` AS `name`,`vitess_a`.`foo` AS `foo` from `vitess_a`\") VARCHAR(\"utf8mb4\") VARCHAR(\"utf8mb4_general_ci\")]]",
		fmt.Sprintf("%v", qr.Rows))

	// show create view.
	qr, err = client.Execute("describe vitess_view", nil)
	require.NoError(t, err)
	require.Equal(t,
		"[[VARCHAR(\"eid\") BLOB(\"bigint\") VARCHAR(\"NO\") BINARY(\"\") BLOB(\"0\") VARCHAR(\"\")] [VARCHAR(\"id\") BLOB(\"int\") VARCHAR(\"NO\") BINARY(\"\") BLOB(\"1\") VARCHAR(\"\")] [VARCHAR(\"name\") BLOB(\"varchar(128)\") VARCHAR(\"YES\") BINARY(\"\") NULL VARCHAR(\"\")] [VARCHAR(\"foo\") BLOB(\"varbinary(128)\") VARCHAR(\"YES\") BINARY(\"\") NULL VARCHAR(\"\")]]",
		fmt.Sprintf("%v", qr.Rows))

	// information schema.
	qr, err = client.Execute("select table_type from information_schema.tables where table_schema = database() and table_name = 'vitess_view'", nil)
	require.NoError(t, err)
	require.Equal(t,
		"[[BINARY(\"VIEW\")]]",
		fmt.Sprintf("%v", qr.Rows))
}

// TestViewAndTableUnique will validate that views and tables should have unique names.
func TestViewAndTableUnique(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer func() {
		_, _ = client.Execute("drop view if exists vitess_view", nil)
		_, _ = client.Execute("drop table if exists vitess_view", nil)
	}()

	// create a view.
	_, err := client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	// should error on create table as view already exists with same name.
	_, err = client.Execute("create table vitess_view(id bigint primary key)", nil)
	require.ErrorContains(t, err, "Table 'vitess_view' already exists")

	// drop the view
	_, err = client.Execute("drop view vitess_view", nil)
	require.NoError(t, err)

	// create the table first.
	_, err = client.Execute("create table vitess_view(id bigint primary key)", nil)
	require.NoError(t, err)

	// create view should fail as table already exists with same name.
	_, err = client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.ErrorContains(t, err, "Table 'vitess_view' already exists")
}

func waitForResult(t *testing.T, client *framework.QueryClient, rowCount int, timeout time.Duration) {
	t.Helper()
	wait := time.After(timeout)
	success := false
	for {
		select {
		case <-wait:
			t.Errorf("all views are not dropped within the time")
			return
		case <-time.After(1 * time.Second):
			qr, err := client.Execute(qSelAllRows, nil)
			require.NoError(t, err)
			if len(qr.Rows) == rowCount {
				success = true
			}
		}
		if success {
			break
		}
	}
}
