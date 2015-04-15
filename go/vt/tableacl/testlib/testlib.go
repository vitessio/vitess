// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/acl"
)

// TestSuite tests a concrete acl.Factory implementation.
func TestSuite(t *testing.T, factory acl.Factory) {
	name := fmt.Sprintf("tableacl-test-%d", rand.Int63())
	tableacl.Register(name, factory)
	tableacl.SetDefaultACL(name)

	testParseInvalidJSON(t)
	testInvalidRoleName(t)
	testInvalidRegex(t)
	testValidConfigs(t)
	testDenyReaderInsert(t)
	testAllowReaderSelect(t)
	testDenyReaderDDL(t)
	testAllowUnmatchedTable(t)
	testAllUserReadAccess(t)
	testAllUserWriteAccess(t)
}

func currentUser() string {
	return "DummyUser"
}

func testParseInvalidJSON(t *testing.T) {
	checkLoad([]byte(`{1:2}`), false, t)
	checkLoad([]byte(`{"1":"2"}`), false, t)
	checkLoad([]byte(`{"table1":{1:2}}`), false, t)
}

func testInvalidRoleName(t *testing.T) {
	checkLoad([]byte(`{"table1":{"SOMEROLE":"u1"}}`), false, t)
}

func testInvalidRegex(t *testing.T) {
	checkLoad([]byte(`{"table(1":{"READER":"u1"}}`), false, t)
}

func testValidConfigs(t *testing.T) {
	checkLoad([]byte(`{"table1":{"READER":"u1"}}`), true, t)
	checkLoad([]byte(`{"table1":{"READER":"u1,u2", "WRITER":"u3"}}`), true, t)
	checkLoad([]byte(`{"table[0-9]+":{"Reader":"u1,u2", "WRITER":"u3"}}`), true, t)
	checkLoad([]byte(`{"table[0-9]+":{"Reader":"u1,`+allString()+`", "WRITER":"u3"}}`), true, t)
	checkLoad([]byte(`{
		"table[0-9]+":{"Reader":"u1,`+allString()+`", "WRITER":"u3"},
		"tbl[0-9]+":{"Reader":"u1,`+allString()+`", "WRITER":"u3", "ADMIN":"u4"}
	}`), true, t)
}

func testDenyReaderInsert(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + currentUser() + `", "WRITER":"u3"}}`)
	checkAccess(configData, "table1", tableacl.WRITER, t, false)
}

func testAllowReaderSelect(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + currentUser() + `", "WRITER":"u3"}}`)
	checkAccess(configData, "table1", tableacl.READER, t, true)
}

func testDenyReaderDDL(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + currentUser() + `", "WRITER":"u3"}}`)
	checkAccess(configData, "table1", tableacl.ADMIN, t, false)
}

func testAllowUnmatchedTable(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + currentUser() + `", "WRITER":"u3"}}`)
	checkAccess(configData, "UNMATCHED_TABLE", tableacl.ADMIN, t, true)
}

func testAllUserReadAccess(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + allString() + `", "WRITER":"u3"}}`)
	checkAccess(configData, "table1", tableacl.READER, t, true)
}

func testAllUserWriteAccess(t *testing.T) {
	configData := []byte(`{"table[0-9]+":{"Reader":"` + currentUser() + `", "WRITER":"` + allString() + `"}}`)
	checkAccess(configData, "table1", tableacl.WRITER, t, true)
}

func checkLoad(configData []byte, valid bool, t *testing.T) {
	err := tableacl.InitFromBytes(configData)
	if !valid && err == nil {
		t.Errorf("expecting parse error none returned")
	}

	if valid && err != nil {
		t.Errorf("unexpected load error: %v", err)
	}
}

func checkAccess(configData []byte, tableName string, role tableacl.Role, t *testing.T, want bool) {
	checkLoad(configData, true, t)
	got := tableacl.Authorized(tableName, role).IsMember(currentUser())
	if want != got {
		t.Errorf("got %v, want %v", got, want)
	}
}

func allString() string {
	return tableacl.GetCurrentAclFactory().AllString()
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
