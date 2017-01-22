// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"errors"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	_ "github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

func TestBatchOrdered(t *testing.T) {
	router, sbc1, sbc2, sbclookup := createRouterEnv()

	sqlList := []string{
		"select id from music_user_map where id = 1",
		"update music_user_map set id = 1",
		"insert into no_table values (1)",
		"delete from music_user_map",
		"insert into music_user_map values (1)",
	}

	_, err := routerExecBatch(router, sqlList, nil, false)
	if err != nil {
		t.Error(err)
	}

	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "update music_user_map set id = 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "delete from music_user_map",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "insert into music_user_map values (1)",
		BindVariables: map[string]interface{}{},
	}}
	if !reflect.DeepEqual(sbclookup.Queries, wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
	wantQueries = nil
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
}

func TestBatchUnOrdered(t *testing.T) {
	router, sbc1, sbc2, sbclookup := createRouterEnv()

	sqlList := []string{
		"select id from music_user_map where id = 1",
		"update music_user_map set id = 1",
		"insert into no_table values (1)",
		"delete from music_user_map",
		"insert into music_user_map values (1)",
	}

	_, err := routerExecBatch(router, sqlList, nil, true)
	if err != nil {
		t.Error(err)
	}

	wantQueries := []querytypes.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "update music_user_map set id = 1",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "delete from music_user_map",
		BindVariables: map[string]interface{}{},
	}, {
		Sql:           "insert into music_user_map values (1)",
		BindVariables: map[string]interface{}{},
	}}

	// Cannot Check with DeepEqual as queries can be in any order.
	if len(sbclookup.Queries) != len(wantQueries) {
		t.Errorf("sbclookup.Queries: %+v, want %+v\n", sbclookup.Queries, wantQueries)
	}
	wantQueries = nil
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
}

func TestBatchFailure(t *testing.T) {
	router, _, _, _ := createRouterEnv()

	sqlList := []string{
		"select id from music_user_map where id = 1",
		"update music_user_map set id = 1",
		"delete from music_user_map",
		"insert into music_user_map values (1)",
	}

	emptyBindVars := []map[string]interface{}{}
	wantErr := errors.New("Query list size does not match bindvars size")
	_, err := routerExecBatch(router, sqlList, emptyBindVars, false)

	if err == wantErr {
		t.Errorf("got: %+v, want %+v\n", err, wantErr)
	}
}
