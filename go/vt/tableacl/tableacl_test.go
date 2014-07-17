// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tableacl

import (
	"testing"

	"github.com/youtube/vitess/go/vt/context"
)

func currentUser() string {
	ctx := &context.DummyContext{}
	return ctx.GetUsername()
}

func TestParseInvalidJSON(t *testing.T) {
	checkLoad([]byte(`{1:2}`), false, t)
	checkLoad([]byte(`{"1":"2"}`), false, t)
	checkLoad([]byte(`{"table1":{1:2}}`), false, t)
}

func TestInvalidRoleName(t *testing.T) {
	checkLoad([]byte(`{"table1":{"SOMEROLE":"u1"}}`), false, t)
}

func TestInvalidRegex(t *testing.T) {
	checkLoad([]byte(`{"table(1":{"READER":"u1"}}`), false, t)
}

func TestValidConfigs(t *testing.T) {
	checkLoad([]byte(`{"table1":{"READER":"u1"}}`), true, t)
	checkLoad([]byte(`{"table1":{"READER":"u1,u2", "WRITER":"u3"}}`), true, t)
	checkLoad([]byte(`{"table[0-9]+":{"Reader":"u1,u2", "WRITER":"u3"}}`), true, t)
	checkLoad([]byte(`{"table[0-9]+":{"Reader":"u1,*", "WRITER":"u3"}}`), true, t)
	checkLoad([]byte(`{
		"table[0-9]+":{"Reader":"u1,*", "WRITER":"u3"},
		"tbl[0-9]+":{"Reader":"u1,*", "WRITER":"u3", "ADMIN":"u4"}
	}`), true, t)

}

func TestDenyReaderInsert(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + currentUser() + `", "WRITER":"u3"}}`)
	checkAccess(configData, "table1", WRITER, t, false)
}

func TestAllowReaderSelect(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + currentUser() + `", "WRITER":"u3"}}`)
	checkAccess(configData, "table1", READER, t, true)
}

func TestDenyReaderDDL(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + currentUser() + `", "WRITER":"u3"}}`)
	checkAccess(configData, "table1", ADMIN, t, false)
}

func TestAllowUnmatchedTable(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + currentUser() + `", "WRITER":"u3"}}`)
	checkAccess(configData, "UNMATCHED_TABLE", ADMIN, t, true)
}

func TestAllUserReadAcess(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"*", "WRITER":"u3"}}`)
	checkAccess(configData, "table1", READER, t, true)
}

func TestAllUserWriteAccess(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + currentUser() + `", "WRITER":"*"}}`)
	checkAccess(configData, "table1", WRITER, t, true)
}

func checkLoad(configData []byte, valid bool, t *testing.T) {
	_, err := load(configData)
	if !valid && err == nil {
		t.Errorf("expecting parse error none returned")
	}

	if valid && err != nil {
		t.Errorf("unexpected load error: %v", err)
	}
}

func checkAccess(configData []byte, tableName string, role Role, t *testing.T, want bool) {
	var err error
	acl, err = load(configData)
	if err != nil {
		t.Errorf("load error: %v", err)
	}
	got := Check(currentUser(), Authorized(tableName, role))
	if want != got {
		t.Errorf("got %v, want %v", got, want)
	}
}
