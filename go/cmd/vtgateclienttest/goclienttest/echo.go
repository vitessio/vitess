/*
Copyright 2019 The Vitess Authors.

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

package goclienttest

import (
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	echoPrefix = "echo://"

	query = "test query with unicode: \u6211\u80fd\u541e\u4e0b\u73bb\u7483\u800c\u4e0d\u50b7\u8eab\u9ad4"

	bindVars = map[string]*querypb.BindVariable{
		"int":   sqltypes.Int64BindVariable(123),
		"float": sqltypes.Float64BindVariable(2.1),
		"bytes": sqltypes.BytesBindVariable([]byte{1, 2, 3}),
	}
	bindVarsEcho = "map[bytes:type:VARBINARY value:\"\\001\\002\\003\"  float:type:FLOAT64 value:\"2.1\"  int:type:INT64 value:\"123\" ]"

	callerID     = callerid.NewEffectiveCallerID("test_principal", "test_component", "test_subcomponent")
	callerIDEcho = "principal:\"test_principal\" component:\"test_component\" subcomponent:\"test_subcomponent\" "
)

// testEcho exercises the test cases provided by the "echo" service.
func testEcho(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	testEchoExecute(t, conn, session)
	testEchoStreamExecute(t, conn, session)
}

func testEchoExecute(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	var qr *sqltypes.Result
	var err error

	ctx := callerid.NewContext(context.Background(), callerID, nil)

	qr, err = session.Execute(ctx, echoPrefix+query, bindVars)
	checkEcho(t, "Execute", qr, err, map[string]string{
		"callerId": callerIDEcho,
		"query":    echoPrefix + query,
		"bindVars": bindVarsEcho,
	})
}

func testEchoStreamExecute(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	var stream sqltypes.ResultStream
	var err error
	var qr *sqltypes.Result

	ctx := callerid.NewContext(context.Background(), callerID, nil)

	stream, err = session.StreamExecute(ctx, echoPrefix+query, bindVars)
	if err != nil {
		t.Fatal(err)
	}
	qr, err = stream.Recv()
	checkEcho(t, "StreamExecute", qr, err, map[string]string{
		"callerId": callerIDEcho,
		"query":    echoPrefix + query,
		"bindVars": bindVarsEcho,
	})
}

// getEcho extracts the echoed field values from a query result.
func getEcho(qr *sqltypes.Result) map[string]sqltypes.Value {
	values := map[string]sqltypes.Value{}
	if qr == nil {
		return values
	}
	for i, field := range qr.Fields {
		values[field.Name] = qr.Rows[0][i]
	}
	return values
}

// checkEcho verifies that the values present in 'want' are equal to those in
// 'got'. Note that extra values in 'got' are fine.
func checkEcho(t *testing.T, name string, qr *sqltypes.Result, err error, want map[string]string) {
	if err != nil {
		t.Fatalf("%v error: %v", name, err)
	}
	got := getEcho(qr)
	for k, v := range want {
		if got[k].ToString() != v {
			t.Errorf("%v: %v = \n%q, want \n%q", name, k, got[k], v)
		}
	}

	// Check NULL and empty string.
	if !got["null"].IsNull() {
		t.Errorf("MySQL NULL value wasn't preserved")
	}
	if !got["emptyString"].IsQuoted() || got["emptyString"].ToString() != "" {
		t.Errorf("Empty string value wasn't preserved: %#v", got)
	}
}
