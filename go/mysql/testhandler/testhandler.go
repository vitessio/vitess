/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package testhandler

import (
	"strings"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

const BenchmarkQueryPrefix = "benchmark "

var SelectRowsResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "id",
			Type: querypb.Type_INT32,
		},
		{
			Name: "name",
			Type: querypb.Type_VARCHAR,
		},
	},
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
		},
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("20")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nicer name")),
		},
	},
	RowsAffected: 2,
}

type TestHandler struct {
	lastConn *mysql.Conn
	result   *sqltypes.Result
	err      error
	warnings uint16
}

func (th *TestHandler) NewConnection(c *mysql.Conn) {
	th.lastConn = c
}

func (th *TestHandler) ConnectionClosed(c *mysql.Conn) {
}

func (th *TestHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	if th.result != nil {
		callback(th.result)
		return nil
	}

	switch query {
	case "error":
		return th.err
	case "panic":
		panic("test panic attack!")
	case "select rows":
		callback(SelectRowsResult)
	case "error after send":
		callback(SelectRowsResult)
		return th.err
	case "insert":
		callback(&sqltypes.Result{
			RowsAffected: 123,
			InsertID:     123456789,
		})
	case "schema echo":
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "schema_name",
					Type: querypb.Type_VARCHAR,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.SchemaName)),
				},
			},
		})
	case "ssl echo":
		value := "OFF"
		if c.Capabilities&mysql.CapabilityClientSSL > 0 {
			value = "ON"
		}
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "ssl_flag",
					Type: querypb.Type_VARCHAR,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(value)),
				},
			},
		})
	case "userData echo":
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "user",
					Type: querypb.Type_VARCHAR,
				},
				{
					Name: "user_data",
					Type: querypb.Type_VARCHAR,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.User)),
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.UserData.Get().Username)),
				},
			},
		})
	default:
		if strings.HasPrefix(query, BenchmarkQueryPrefix) {
			callback(&sqltypes.Result{
				Fields: []*querypb.Field{
					{
						Name: "result",
						Type: querypb.Type_VARCHAR,
					},
				},
				Rows: [][]sqltypes.Value{
					{
						sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(query)),
					},
				},
			})
		}

		callback(&sqltypes.Result{})
	}
	return nil
}

func (th *TestHandler) WarningCount(c *mysql.Conn) uint16 {
	return th.warnings
}
