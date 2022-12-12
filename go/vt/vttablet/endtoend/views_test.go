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

// Test will validate create view ddls.
func TestCreateViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute("delete from _vt.views", nil)

	_, err := client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	// validate the row in _vt.views.
	qr, err := client.Execute("select * from _vt.views", nil)
	require.NoError(t, err)
	require.Equal(t,
		`[[VARCHAR("vitess_view") TEXT("select * from vitess_a") TEXT("create view vitess_view as select * from vitess_a")]]`,
		fmt.Sprintf("%v", qr.Rows))

	// view already exists. This should fail.
	_, err = client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.ErrorContains(t, err, "View 'vitess_view' already exists")

	// view already exists, but create or replace syntax should allow it to replace the view.
	_, err = client.Execute("create or replace view vitess_view as select id, foo from vitess_a", nil)
	require.NoError(t, err)

	// validate the row in _vt.views.
	qr, err = client.Execute("select * from _vt.views", nil)
	require.NoError(t, err)
	require.Equal(t,
		`[[VARCHAR("vitess_view") TEXT("select id, foo from vitess_a") TEXT("create or replace view vitess_view as select id, foo from vitess_a")]]`,
		fmt.Sprintf("%v", qr.Rows))
}

// Test will validate alter view ddls.
func TestAlterViewDDL(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	defer client.Execute("delete from _vt.views", nil)

	// view does not exist, show FAIL
	_, err := client.Execute("alter view vitess_view as select * from vitess_a", nil)
	require.ErrorContains(t, err, "View 'vitess_view' does not exist")

	// create a view.
	_, err = client.Execute("create view vitess_view as select * from vitess_a", nil)
	require.NoError(t, err)

	// view exists, should PASS
	_, err = client.Execute("alter view vitess_view as select id, foo from vitess_a", nil)
	require.NoError(t, err)

	// validate the row in _vt.views.
	qr, err := client.Execute("select * from _vt.views", nil)
	require.NoError(t, err)
	require.Equal(t,
		`[[VARCHAR("vitess_view") TEXT("select id, foo from vitess_a") TEXT("create view vitess_view as select id, foo from vitess_a")]]`,
		fmt.Sprintf("%v", qr.Rows))
}
