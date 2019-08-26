/*
Copyright 2017 Google Inc.

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

package vtgate

import (
	"net"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestMySQLProtocolExecute(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	c, err := mysqlConnect(&mysql.ConnParams{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	qr, err := c.ExecuteFetch("select id from t1", 10, true /* wantfields */)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}

	options := &querypb.ExecuteOptions{
		IncludedFields: querypb.ExecuteOptions_ALL,
	}
	if !proto.Equal(sbc.Options[0], options) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], options)
	}
}

func TestMySQLProtocolStreamExecute(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	c, err := mysqlConnect(&mysql.ConnParams{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.ExecuteFetch("set workload='olap'", 1, true /* wantfields */)
	if err != nil {
		t.Error(err)
	}

	qr, err := c.ExecuteFetch("select id from t1", 10, true /* wantfields */)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}

	options := &querypb.ExecuteOptions{
		IncludedFields: querypb.ExecuteOptions_ALL,
		Workload:       querypb.ExecuteOptions_OLAP,
	}
	if !proto.Equal(sbc.Options[0], options) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], options)
	}
}

func TestMySQLProtocolExecuteUseStatement(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	c, err := mysqlConnect(&mysql.ConnParams{DbName: "@master"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	qr, err := c.ExecuteFetch("show vitess_target", 1, false)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "VARCHAR(\"@master\")", qr.Rows[0][0].String())

	_, err = c.ExecuteFetch("use TestUnsharded", 0, false)
	if err != nil {
		t.Error(err)
	}

	qr, err = c.ExecuteFetch("show vitess_target", 1, false)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "VARCHAR(\"TestUnsharded\")", qr.Rows[0][0].String())
}

func TestMysqlProtocolInvalidDB(t *testing.T) {
	c, err := mysqlConnect(&mysql.ConnParams{DbName: "invalidDB"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.ExecuteFetch("select id from t1", 10, true /* wantfields */)
	c.Close()
	want := "vtgate: : keyspace invalidDB not found in vschema (errno 1105) (sqlstate HY000) during query: select id from t1"
	if err == nil || err.Error() != want {
		t.Errorf("exec with db:\n%v, want\n%s", err, want)
	}
}

func TestMySQLProtocolClientFoundRows(t *testing.T) {
	createSandbox(KsTestUnsharded)
	hcVTGateTest.Reset()
	sbc := hcVTGateTest.AddTestTablet("aa", "1.1.1.1", 1001, KsTestUnsharded, "0", topodatapb.TabletType_MASTER, true, 1, nil)

	c, err := mysqlConnect(&mysql.ConnParams{Flags: mysql.CapabilityClientFoundRows})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	qr, err := c.ExecuteFetch("select id from t1", 10, true /* wantfields */)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(sandboxconn.SingleRowResult, qr) {
		t.Errorf("want \n%+v, got \n%+v", sandboxconn.SingleRowResult, qr)
	}

	options := &querypb.ExecuteOptions{
		IncludedFields:  querypb.ExecuteOptions_ALL,
		ClientFoundRows: true,
	}
	if !proto.Equal(sbc.Options[0], options) {
		t.Errorf("got ExecuteOptions \n%+v, want \n%+v", sbc.Options[0], options)
	}
}

// mysqlConnect fills the host & port into params and connects
// to the mysql protocol port.
func mysqlConnect(params *mysql.ConnParams) (*mysql.Conn, error) {
	host, port, err := net.SplitHostPort(mysqlListener.Addr().String())
	if err != nil {
		return nil, err
	}
	portnum, _ := strconv.Atoi(port)
	params.Host = host
	params.Port = portnum
	return mysql.Connect(context.Background(), params)
}
