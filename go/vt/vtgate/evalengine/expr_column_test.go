/*
Copyright 2023 The Vitess Authors.

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

package evalengine

import (
	"testing"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestTypeOf(t *testing.T) {
	env := &ExpressionEnv{
		BindVars: make(map[string]*querypb.BindVariable),
	}

	field1 := &querypb.Field{
		Name:  "field1",
		Type:  querypb.Type_INT64,
		Flags: uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
	}
	field2 := &querypb.Field{
		Name:  "field2",
		Type:  querypb.Type_VARCHAR,
		Flags: 0,
	}
	fields := []*querypb.Field{field1, field2}

	c := &Column{}
	env.Row = sqltypes.Row{sqltypes.NewInt64(10)}
	env.Fields = fields

	t.Run("Check when row value is not null", func(t *testing.T) {
		typ, f := c.typeof(env)
		if typ != sqltypes.Int64 || f != flag(0) {
			t.Errorf("typeof() failed, expected sqltypes.Int64 and typeFlag 0, got %v and %v", typ, f)
		}
	})

	t.Run("Check when row value is null", func(t *testing.T) {
		env.Row = sqltypes.Row{
			sqltypes.NULL,
		}
		typ, flag := c.typeof(env)
		if typ != querypb.Type_INT64 || flag != flagNullable {
			t.Errorf("typeof() failed, expected querypb.Type_INT64 and flagNullable, got %v and %v", typ, flag)
		}
	})
}
