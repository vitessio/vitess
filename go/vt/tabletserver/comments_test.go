// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

func TestComments(t *testing.T) {
	query := proto.Query{
		Sql:           "foo /*** bar */",
		BindVariables: make(map[string]interface{}),
	}
	stripTrailing(&query)
	want := proto.Query{
		Sql: "foo",
		BindVariables: map[string]interface{}{
			"_trailingComment": " /*** bar */",
		},
	}
	if !reflect.DeepEqual(query, want) {
		t.Errorf("got\n%+v, want\n%+v", query, want)
	}
}
