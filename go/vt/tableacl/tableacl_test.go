// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tableacl

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/testfiles"
	"github.com/youtube/vitess/go/vt/tableacl/acl"
	"github.com/youtube/vitess/go/vt/tableacl/simpleacl"
)

type fakeAclFactory struct{}

func (factory *fakeAclFactory) New(entries []string) (acl.ACL, error) {
	return nil, fmt.Errorf("unable to create a new ACL")
}

func (factory *fakeAclFactory) All() acl.ACL {
	return &fakeACL{}
}

// AllString returns a string representation of all users.
func (factory *fakeAclFactory) AllString() string {
	return ""
}

type fakeACL struct{}

func (acl *fakeACL) IsMember(principal string) bool {
	return false
}

func TestInitWithInvalidFilePath(t *testing.T) {
	setUpTableACL(&simpleacl.Factory{})
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("init should fail for an invalid config file path")
		}
	}()
	Init("/invalid_file_path")
}

func TestInitWithInvalidConfigFile(t *testing.T) {
	setUpTableACL(&simpleacl.Factory{})
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("init should fail for an invalid config file")
		}
	}()
	Init(testfiles.Locate("tableacl/invalid_tableacl_config.json"))
}

func TestInitWithValidConfig(t *testing.T) {
	setUpTableACL(&simpleacl.Factory{})
	Init(testfiles.Locate("tableacl/test_table_tableacl_config.json"))
}

func TestInitFromBytes(t *testing.T) {
	aclFactory := &simpleacl.Factory{}
	setUpTableACL(aclFactory)
	acl := Authorized("test_table", READER)
	if acl != nil {
		t.Fatalf("tableacl has not been initialized, should get nil ACL")
	}
	err := InitFromBytes([]byte(`{"test_table":{"Reader": "vt"}}`))
	if err != nil {
		t.Fatalf("tableacl init should succeed, but got error: %v", err)
	}

	acl = Authorized("unknown_table", READER)
	if !reflect.DeepEqual(aclFactory.All(), acl) {
		t.Fatalf("there is no config for unknown_table, should grand all permission")
	}

	acl = Authorized("test_table", READER)
	if !acl.IsMember("vt") {
		t.Fatalf("user: vt should have reader permission to table: test_table")
	}
}

func TestInvalidTableRegex(t *testing.T) {
	setUpTableACL(&simpleacl.Factory{})
	err := InitFromBytes([]byte(`{"table(":{"Reader": "vt", "WRITER":"vt"}}`))
	if err == nil {
		t.Fatalf("tableacl init should fail because config file has an invalid table regex")
	}
}

func TestInvalidRole(t *testing.T) {
	setUpTableACL(&simpleacl.Factory{})
	err := InitFromBytes([]byte(`{"test_table":{"InvalidRole": "vt", "Reader": "vt", "WRITER":"vt"}}`))
	if err == nil {
		t.Fatalf("tableacl init should fail because config file has an invalid role")
	}
}

func TestFailedToCreateACL(t *testing.T) {
	setUpTableACL(&fakeAclFactory{})
	err := InitFromBytes([]byte(`{"test_table":{"Reader": "vt", "WRITER":"vt"}}`))
	if err == nil {
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
	if !reflect.DeepEqual(aclFactory, GetCurrentAclFactory()) {
		t.Fatalf("should return registered acl factory even if default acl is not set.")
	}
	Register(name+"2", aclFactory)
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("there are more than one acl factories, but the default is not set")
		}
	}()
	GetCurrentAclFactory()
}

func TestGetAclFactoryWithWrongDefault(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := fmt.Sprintf("tableacl-name-%d", rand.Int63())
	aclFactory := &simpleacl.Factory{}
	Register(name, aclFactory)
	Register(name+"2", aclFactory)
	SetDefaultACL("wrong_name")
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("there are more than one acl factories, but the default given does not match any of these.")
		}
	}()
	GetCurrentAclFactory()
}

func setUpTableACL(factory acl.Factory) {
	tableAcl = nil
	name := fmt.Sprintf("tableacl-name-%d", rand.Int63())
	Register(name, factory)
	SetDefaultACL(name)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
