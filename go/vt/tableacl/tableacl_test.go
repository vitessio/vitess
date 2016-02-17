// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tableacl

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	tableaclpb "github.com/youtube/vitess/go/vt/proto/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/acl"
	"github.com/youtube/vitess/go/vt/tableacl/simpleacl"
)

type fakeAclFactory struct{}

func (factory *fakeAclFactory) New(entries []string) (acl.ACL, error) {
	return nil, fmt.Errorf("unable to create a new ACL")
}

type fakeACL struct{}

func (acl *fakeACL) IsMember(principal string) bool {
	return false
}

func TestInitWithInvalidFilePath(t *testing.T) {
	setUpTableACL(&simpleacl.Factory{})
	if err := Init("/invalid_file_path", func() {}); err == nil {
		t.Fatalf("init should fail for an invalid config file path")
	}
}

var aclJSON = `{
  "table_groups": [
    {
      "name": "group01",
      "table_names_or_prefixes": ["test_table"],
      "readers": ["vt"],
      "writers": ["vt"]
    }
  ]
}`

func TestInitWithValidConfig(t *testing.T) {
	setUpTableACL(&simpleacl.Factory{})
	f, err := ioutil.TempFile("", "tableacl")
	if err != nil {
		t.Error(err)
		return
	}
	defer os.Remove(f.Name())
	n, err := f.WriteString(aclJSON)
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(aclJSON) {
		t.Error("short write")
		return
	}
	err = f.Close()
	if err != nil {
		t.Error(err)
		return
	}
	Init(f.Name(), func() {})
}

func TestInitFromProto(t *testing.T) {
	setUpTableACL(&simpleacl.Factory{})
	readerACL := Authorized("my_test_table", READER)
	want := &ACLResult{ACL: acl.DenyAllACL{}, GroupName: ""}
	if !reflect.DeepEqual(readerACL, want) {
		t.Fatalf("tableacl has not been initialized, got: %v, want: %v", readerACL, want)
	}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"vt"},
		}},
	}
	if err := InitFromProto(config); err != nil {
		t.Fatalf("tableacl init should succeed, but got error: %v", err)
	}

	if !reflect.DeepEqual(GetCurrentConfig(), config) {
		t.Fatalf("GetCurrentConfig() = %v, want: %v", GetCurrentConfig(), config)
	}

	readerACL = Authorized("unknown_table", READER)
	if !reflect.DeepEqual(readerACL, want) {
		t.Fatalf("there is no config for unknown_table, should deny by default")
	}

	readerACL = Authorized("test_table", READER)
	if !readerACL.IsMember("vt") {
		t.Fatalf("user: vt should have reader permission to table: test_table")
	}
}

func TestTableACLValidateConfig(t *testing.T) {
	if err := validate(nil); err != nil {
		t.Fatalf("validate(<entries>) = %v, want: nil", err)
	}
	if err := validate([]aclEntry{}); err != nil {
		t.Fatalf("validate(<entries>) = %v, want: nil", err)
	}
	if err := validate([]aclEntry{{tableNameOrPrefix: "b"}}); err != nil {
		t.Fatalf("validate(<entries>) = %v, want: nil", err)
	}
	if err := validate([]aclEntry{{tableNameOrPrefix: "b%c"}}); err == nil {
		t.Fatal("validate(<entries>) = nil, want: error")
	}

	entries := []aclEntry{
		{tableNameOrPrefix: "b"},
		{tableNameOrPrefix: "a"},
	}
	// error because entries are not sorted by tableNameOrPrefix
	if err := validate(entries); err == nil {
		t.Fatal("validate(<entries>) = nil, want: error")
	}

	entries = []aclEntry{
		{tableNameOrPrefix: "aaa"},
		{tableNameOrPrefix: "aaab"},
		{tableNameOrPrefix: "aaab%"},
		{tableNameOrPrefix: "aaabb"},
	}
	// error because two entries overlaps
	if err := validate(entries); err == nil {
		t.Fatal("validate(<entries>) = nil, want: error")
	}
	entries = []aclEntry{
		{tableNameOrPrefix: "a"},
		{tableNameOrPrefix: "aa%"},
		{tableNameOrPrefix: "aaab%"},
	}
	// error because two entries overlaps
	if err := validate(entries); err == nil {
		t.Fatal("validate(<entries>) = nil, want: error")
	}
	entries = []aclEntry{
		{tableNameOrPrefix: "a"},
		{tableNameOrPrefix: "aa%"},
		{tableNameOrPrefix: "aaab"},
		{tableNameOrPrefix: "aaabb"},
	}
	// error because two entries overlaps
	if err := validate(entries); err == nil {
		t.Fatal("validate(<entries>) = nil, want: error")
	}
	entries = []aclEntry{
		{tableNameOrPrefix: "a"},
		{tableNameOrPrefix: "aa"},
		{tableNameOrPrefix: "aaa%%"},
		{tableNameOrPrefix: "aaaaa"},
	}
	// error because there is an invalid entry.
	if err := validate(entries); err == nil {
		t.Fatal("validate(<entries>) = nil, want: error")
	}
	entries = []aclEntry{
		{tableNameOrPrefix: "a"},
		{tableNameOrPrefix: "aa"},
		{tableNameOrPrefix: "aa"},
		{tableNameOrPrefix: "aaaaa"},
	}
	// error because there are duplicate entries.
	if err := validate(entries); err == nil {
		t.Fatal("validate(<entries>) = nil, want: error")
	}
}

func TestTableACLAuthorize(t *testing.T) {
	setUpTableACL(&simpleacl.Factory{})
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{
			{
				Name:                 "group01",
				TableNamesOrPrefixes: []string{"test_music"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
			{
				Name:                 "group02",
				TableNamesOrPrefixes: []string{"test_music_02", "test_video"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u3"},
				Admins:               []string{"u4"},
			},
			{
				Name:                 "group03",
				TableNamesOrPrefixes: []string{"test_other%"},
				Readers:              []string{"u2"},
				Writers:              []string{"u2", "u3"},
				Admins:               []string{"u3"},
			},
			{
				Name:                 "group04",
				TableNamesOrPrefixes: []string{"test_data%"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
		},
	}
	if err := InitFromProto(config); err != nil {
		t.Fatalf("InitFromProto(<data>) = %v, want: nil", err)
	}

	readerACL := Authorized("test_data_any", READER)
	if !readerACL.IsMember("u1") {
		t.Fatalf("user u1 should have reader permission to table test_data_any")
	}
	if !readerACL.IsMember("u2") {
		t.Fatalf("user u2 should have reader permission to table test_data_any")
	}
}

func TestFailedToCreateACL(t *testing.T) {
	setUpTableACL(&fakeAclFactory{})
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"vt"},
			Writers:              []string{"vt"},
		}},
	}
	if err := InitFromProto(config); err == nil {
		t.Fatalf("tableacl init should fail because fake ACL returns an error")
	}
}

func TestDoubleRegisterTheSameKey(t *testing.T) {
	acls = make(map[string]acl.Factory)
	name := fmt.Sprintf("tableacl-name-%d", rand.Int63())
	Register(name, &simpleacl.Factory{})
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("the second tableacl register should fail")
		}
	}()
	Register(name, &simpleacl.Factory{})
}

func TestGetAclFactory(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := fmt.Sprintf("tableacl-name-%d", rand.Int63())
	aclFactory := &simpleacl.Factory{}
	Register(name, aclFactory)
	f, err := GetCurrentAclFactory()
	if err != nil {
		t.Errorf("Fail to get current ACL Factory: %v", err)
	}
	if !reflect.DeepEqual(aclFactory, f) {
		t.Fatalf("should return registered acl factory even if default acl is not set.")
	}
	Register(name+"2", aclFactory)
	_, err = GetCurrentAclFactory()
	if err == nil {
		t.Fatalf("there are more than one acl factories, but the default is not set")
	}
}

func TestGetAclFactoryWithWrongDefault(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := fmt.Sprintf("tableacl-name-%d", rand.Int63())
	aclFactory := &simpleacl.Factory{}
	Register(name, aclFactory)
	Register(name+"2", aclFactory)
	SetDefaultACL("wrong_name")
	_, err := GetCurrentAclFactory()
	if err == nil {
		t.Fatalf("there are more than one acl factories, but the default given does not match any of these.")
	}
}

func setUpTableACL(factory acl.Factory) {
	name := fmt.Sprintf("tableacl-name-%d", rand.Int63())
	Register(name, factory)
	SetDefaultACL(name)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
