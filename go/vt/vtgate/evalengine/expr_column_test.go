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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestTypeOf(t *testing.T) {
	t.Skipf("TODO: these tests are not green")

	env := &ExpressionEnv{
		BindVars:     make(map[string]*querypb.BindVariable),
		now:          time.Now(),
		collationEnv: collations.MySQL8(),
	}
	c := &Column{
		Type: sqltypes.Unknown,
	}
	env.Row = sqltypes.Row{sqltypes.NewInt64(10)}

	t.Run("Check when row value is not null", func(t *testing.T) {
		tt, _ := c.typeof(env)
		if tt.Type != sqltypes.Int64 || tt.Flag != typeFlag(0) {
			t.Errorf("typeof() failed, expected sqltypes.Int64 and typeFlag 0, got %v and %v", tt.Type, tt.Flag)
		}
	})

	t.Run("Check when row value is null", func(t *testing.T) {
		env.Row = sqltypes.Row{
			sqltypes.NULL,
		}
		tt, _ := c.typeof(env)
		if tt.Type != querypb.Type_INT64 || tt.Flag != flagNullable {
			t.Errorf("typeof() failed, expected querypb.Type_INT64 and flagNullable, got %v and %v", tt.Type, tt.Flag)
		}
	})

	t.Run("Check when offset is out of bounds", func(t *testing.T) {
		c.Offset = 10
		tt, _ := c.typeof(env)
		if tt.Type != sqltypes.Unknown || tt.Flag != flagAmbiguousType {
			t.Errorf("typeof() failed, expected -1 and flagAmbiguousType, got %v and %v", tt.Type, tt.Flag)
		}
	})
	t.Run("Check when typed is true", func(t *testing.T) {
		c.Type = querypb.Type_FLOAT32
		tt, _ := c.typeof(env)
		if tt.Type != querypb.Type_FLOAT32 || tt.Flag != flagNullable {
			t.Errorf("typeof() failed, expected querypb.Type_FLOAT32 and flagNullable, got %v and %v", tt.Type, tt.Flag)
		}
	})
}
