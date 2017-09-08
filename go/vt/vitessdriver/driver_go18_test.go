// +build go1.8

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

// TODO(sougou): delete go1.7 tests after 1.8 becomes mainstream.

package vitessdriver

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"

	"golang.org/x/net/context"
)

func TestBeginIsolation(t *testing.T) {
	db, err := Open(testAddress, "@master", 30*time.Second)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	_, err = db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	want := errIsolationUnsupported.Error()
	if err == nil || err.Error() != want {
		t.Errorf("Begin: %v, want %s", err, want)
	}
}

func TestBindVars(t *testing.T) {
	var testcases = []struct {
		desc   string
		in     []driver.NamedValue
		out    map[string]*querypb.BindVariable
		outErr string
	}{{
		desc: "all names",
		in: []driver.NamedValue{{
			Name:  "n1",
			Value: int64(0),
		}, {
			Name:  "n2",
			Value: "abcd",
		}},
		out: map[string]*querypb.BindVariable{
			"n1": sqltypes.Int64BindVariable(0),
			"n2": sqltypes.StringBindVariable("abcd"),
		},
	}, {
		desc: "prefixed names",
		in: []driver.NamedValue{{
			Name:  ":n1",
			Value: int64(0),
		}, {
			Name:  "@n2",
			Value: "abcd",
		}},
		out: map[string]*querypb.BindVariable{
			"n1": sqltypes.Int64BindVariable(0),
			"n2": sqltypes.StringBindVariable("abcd"),
		},
	}, {
		desc: "all positional",
		in: []driver.NamedValue{{
			Ordinal: 1,
			Value:   int64(0),
		}, {
			Ordinal: 2,
			Value:   "abcd",
		}},
		out: map[string]*querypb.BindVariable{
			"v1": sqltypes.Int64BindVariable(0),
			"v2": sqltypes.StringBindVariable("abcd"),
		},
	}, {
		desc: "name, then position",
		in: []driver.NamedValue{{
			Name:  "n1",
			Value: int64(0),
		}, {
			Ordinal: 2,
			Value:   "abcd",
		}},
		outErr: errNoIntermixing.Error(),
	}, {
		desc: "position, then name",
		in: []driver.NamedValue{{
			Ordinal: 1,
			Value:   int64(0),
		}, {
			Name:  "n2",
			Value: "abcd",
		}},
		outErr: errNoIntermixing.Error(),
	}}

	converter := &converter{}

	for _, tc := range testcases {
		bv, err := converter.bindVarsFromNamedValues(tc.in)
		if bv != nil {
			if !reflect.DeepEqual(bv, tc.out) {
				t.Errorf("%s: %v, want %v", tc.desc, bv, tc.out)
			}
		} else {
			if err == nil || err.Error() != tc.outErr {
				t.Errorf("%s: %v, want %v", tc.desc, err, tc.outErr)
			}
		}
	}
}
