/*
Copyright 2021 The Vitess Authors.

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

package engine

import (
	"fmt"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Apply Primitive is used to apply function on the result.
var _ Primitive = (*Apply)(nil)

//Apply specified the parameter for concatenate primitive
type Apply struct {
	Keyspace *vindexes.Keyspace
	Dest     key.Destination
}

func (a *Apply) RouteType() string {
	panic("implement me")
}

func (a *Apply) GetKeyspaceName() string {
	panic("implement me")
}

func (a *Apply) GetTableName() string {
	panic("implement me")
}

func (a *Apply) Execute(vcursor VCursor, _ map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(a.Keyspace.Name, nil, []key.Destination{a.Dest})
	if err != nil {
		return nil, err
	}
	var queries []*querypb.BoundQuery

	for _, rs := range rss {
		queries = append(queries, &querypb.BoundQuery{
			Sql: fmt.Sprintf("select %s, @@global.gtid_executed", rs.Target.Shard),
		})
	}
	qr, errs := vcursor.ExecuteMultiShard(rss, queries, false, false)
	err = vterrors.Aggregate(errs)
	if err != nil {
		return nil, err
	}

	vgtid := binlogdatapb.VGtid{}
	for _, row := range qr.Rows {
		vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{
			Keyspace: a.Keyspace.Name,
			Shard:    row[0].ToString(),
			Gtid:     row[1].ToString(),
			TablePKs: nil,
		})
	}
	rows := [][]sqltypes.Value{{sqltypes.NewVarChar(vgtid.String())}}

	fields := []*querypb.Field{{
		Name:    "global vgtid_executed",
		Type:    sqltypes.VarChar,
		Charset: mysql.CharacterSetUtf8,
		Flags:   uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
	}}
	return &sqltypes.Result{
		Fields: fields,
		Rows:   rows,
	}, nil

}

func (a *Apply) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (a *Apply) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (a *Apply) NeedsTransaction() bool {
	panic("implement me")
}

func (a *Apply) Inputs() []Primitive {
	panic("implement me")
}

func (a *Apply) description() PrimitiveDescription {
	panic("implement me")
}
