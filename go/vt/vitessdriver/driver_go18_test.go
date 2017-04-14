// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build go1.8

// TODO(sougou): delete go1.7 tests after 1.8 becomes mainstream.

package vitessdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

func TestBeginIsolation(t *testing.T) {
	db, err := Open(testAddress, "", "master", 30*time.Second)
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
		out    map[string]interface{}
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
		out: map[string]interface{}{
			"n1": int64(0),
			"n2": "abcd",
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
		out: map[string]interface{}{
			"n1": int64(0),
			"n2": "abcd",
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
		out: map[string]interface{}{
			"v1": int64(0),
			"v2": "abcd",
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
	for _, tc := range testcases {
		bv, err := bindVarsFromNamedValues(tc.in)
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
