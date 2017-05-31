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

package endtoend

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/vttablet/endtoend/framework"
)

func TestSequence(t *testing.T) {
	want := sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "nextval",
			Type: sqltypes.Int64,
		}},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("0")),
		}},
	}
	for wantval := int64(1); wantval < 10; wantval += 2 {
		want.Rows[0][0] = sqltypes.MakeTrusted(sqltypes.Int64, strconv.AppendInt(nil, wantval, 10))
		qr, err := framework.NewClient().Execute("select next 2 values from vitess_seq", nil)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(*qr, want) {
			t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
		}
	}
	// Verify that the table got updated according to chunk size.
	want = sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("13")),
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("3")),
		}},
	}
	qr, err := framework.NewClient().Execute("select next_id, cache from vitess_seq", nil)
	if err != nil {
		t.Error(err)
		return
	}
	qr.Fields = nil
	if !reflect.DeepEqual(*qr, want) {
		t.Errorf("Execute: \n%#v, want \n%#v", *qr, want)
	}
}
