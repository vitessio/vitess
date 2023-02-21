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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var qSelAllRows = "select table_schema, table_name, view_definition, create_statement from _vt.views"
var qDelAllRows = "delete from _vt.views"

// Test will validate create view ddls.
func TestCreateViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute(qDelAllRows, nil)

	_, err := client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	// validate the row in _vt.views.
	qr, err := client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Equal(t,
		`[[VARCHAR("vttest") VARCHAR("vitess_view") TEXT("select * from vitess_a") TEXT("create view vitess_view as select * from vitess_a")]]`,
		fmt.Sprintf("%v", qr.Rows))

	// view already exists. This should fail.
	_, err = client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.ErrorContains(t, err, "View 'vitess_view' already exists")

	// view already exists, but create or replace syntax should allow it to replace the view.
	_, err = client.Execute("create or replace view vitess_view as select id, foo from vitess_a", nil)
	require.NoError(t, err)

	// validate the row in _vt.views.
	qr, err = client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Equal(t,
		`[[VARCHAR("vttest") VARCHAR("vitess_view") TEXT("select id, foo from vitess_a") TEXT("create or replace view vitess_view as select id, foo from vitess_a")]]`,
		fmt.Sprintf("%v", qr.Rows))
}

// Test will validate alter view ddls.
func TestAlterViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute(qDelAllRows, nil)

	// view does not exist, should FAIL
	_, err := client.Execute("alter view vitess_view as select * from vitess_a", nil)
	require.ErrorContains(t, err, "View 'vitess_view' does not exist")

	// create a view.
	_, err = client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	// view exists, should PASS
	_, err = client.Execute("alter view vitess_view as select id, foo from vitess_a", nil)
	require.NoError(t, err)

	// validate the row in _vt.views.
	qr, err := client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Equal(t,
		`[[VARCHAR("vttest") VARCHAR("vitess_view") TEXT("select id, foo from vitess_a") TEXT("create view vitess_view as select id, foo from vitess_a")]]`,
		fmt.Sprintf("%v", qr.Rows))
}

// Test will validate drop view ddls.
func TestDropViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute(qDelAllRows, nil)

	// view does not exist, should FAIL
	_, err := client.Execute("drop view vitess_view", nil)
	require.ErrorContains(t, err, "Unknown view 'vitess_view'")

	// view does not exist, using if exists clause, should PASS
	_, err = client.Execute("drop view if exists vitess_view", nil)
	require.NoError(t, err)

	// create two views.
	_, err = client.Execute("create view vitess_view1 as select * from vitess_a", nil)
	require.NoError(t, err)
	_, err = client.Execute("create view vitess_view2 as select * from vitess_a", nil)
	require.NoError(t, err)

	// drop vitess_view1, should PASS
	_, err = client.Execute("drop view vitess_view1", nil)
	require.NoError(t, err)

	// drop three views, only vitess_view2 exists. This should FAIL but drops the existing view.
	_, err = client.Execute("drop view vitess_view1, vitess_view2, vitess_view3", nil)
	require.ErrorContains(t, err, "Unknown view 'vitess_view1,vitess_view3'")

	// validate ZERO rows in _vt.views.
	qr, err := client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Zero(t, qr.Rows)

	// create a view.
	_, err = client.Execute("create view vitess_view1 as select * from vitess_a", nil)
	require.NoError(t, err)

	// drop three views with if exists clause, only vitess_view1 exists. This should PASS but drops the existing view.
	_, err = client.Execute("drop view if exists vitess_view1, vitess_view2, vitess_view3", nil)
	require.NoError(t, err)

	// validate ZERO rows in _vt.views.
	qr, err = client.Execute(qSelAllRows, nil)
	require.NoError(t, err)
	require.Zero(t, qr.Rows)
}

// TestGetSchemaRPC will validate GetSchema rpc..
func TestGetSchemaRPC(t *testing.T) {
	client := framework.NewClient()

	viewSchemaDef, err := client.GetSchema(querypb.SchemaTableType_VIEWS)
	require.NoError(t, err)
	require.Zero(t, len(viewSchemaDef))

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute(qDelAllRows, nil)

	_, err = client.Execute("create view vitess_view as select 1 from vitess_a", nil)
	require.NoError(t, err)

	viewSchemaDef, err = client.GetSchema(querypb.SchemaTableType_VIEWS)
	require.NoError(t, err)
	require.Equal(t, viewSchemaDef["vitess_view"], "select 1 from vitess_a")
}
