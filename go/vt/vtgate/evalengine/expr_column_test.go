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
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestTypeOf(t *testing.T) {
	env := &ExpressionEnv{
		BindVars: make(map[string]*querypb.BindVariable),
		now:      time.Now(),
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

	t.Run("Check when row value is not null", func(t *testing.T) {
		typ, flag := c.typeof(env, fields)
		if typ != sqltypes.Int64 || flag != typeFlag(0) {
			t.Errorf("typeof() failed, expected sqltypes.Int64 and typeFlag 0, got %v and %v", typ, flag)
		}
	})

	t.Run("Check when row value is null", func(t *testing.T) {
		env.Row = sqltypes.Row{
			sqltypes.NULL,
		}
		typ, flag := c.typeof(env, fields)
		if typ != querypb.Type_INT64 || flag != flagNullable {
			t.Errorf("typeof() failed, expected querypb.Type_INT64 and flagNullable, got %v and %v", typ, flag)
		}
	})

	t.Run("Check when offset is out of bounds", func(t *testing.T) {
		c.Offset = 10
		typ, flag := c.typeof(env, fields)
		if typ != sqltypes.Null || flag != flagAmbiguousType {
			t.Errorf("typeof() failed, expected sqltypes.Null and flagAmbiguousType, got %v and %v", typ, flag)
		}
	})
	t.Run("Check when typed is true", func(t *testing.T) {
		c.typed = true
		c.Type = querypb.Type_FLOAT32
		typ, flag := c.typeof(env, fields)
		if typ != querypb.Type_FLOAT32 || flag != flagNullable {
			t.Errorf("typeof() failed, expected querypb.Type_FLOAT32 and flagNullable, got %v and %v", typ, flag)
		}
	})
}
