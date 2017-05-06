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

package testlib

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tableaclpb "github.com/youtube/vitess/go/vt/proto/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/acl"
)

// TestSuite tests a concrete acl.Factory implementation.
func TestSuite(t *testing.T, factory acl.Factory) {
	name := fmt.Sprintf("tableacl-test-%d", rand.Int63())
	tableacl.Register(name, factory)
	tableacl.SetDefaultACL(name)

	testValidConfigs(t)
	testDenyReaderInsert(t)
	testAllowReaderSelect(t)
	testDenyReaderDDL(t)
	testAllowUnmatchedTable(t)
}

var currentUser = "DummyUser"

func testValidConfigs(t *testing.T) {
	config := newConfigProto("group01", []string{"table1"}, []string{"u1"}, []string{"vt"}, []string{})
	if err := checkLoad(config, true); err != nil {
		t.Fatal(err)
	}
	config = newConfigProto(
		"group01", []string{"table1"}, []string{"u1", "u2"}, []string{"u3"}, []string{})
	if err := checkLoad(config, true); err != nil {
		t.Fatal(err)
	}
	config = newConfigProto(
		"group01", []string{"table%"}, []string{"u1", "u2"}, []string{"u3"}, []string{})
	if err := checkLoad(config, true); err != nil {
		t.Fatal(err)
	}
}

func testDenyReaderInsert(t *testing.T) {
	config := newConfigProto(
		"group01", []string{"table%"}, []string{currentUser}, []string{"u3"}, []string{})
	if err := checkAccess(config, "table1", tableacl.WRITER, false); err != nil {
		t.Fatal(err)
	}
}

func testAllowReaderSelect(t *testing.T) {
	config := newConfigProto(
		"group01", []string{"table%"}, []string{currentUser}, []string{"u3"}, []string{})
	if err := checkAccess(config, "table1", tableacl.READER, true); err != nil {
		t.Fatal(err)
	}
}

func testDenyReaderDDL(t *testing.T) {
	config := newConfigProto(
		"group01", []string{"table%"}, []string{currentUser}, []string{"u3"}, []string{})
	if err := checkAccess(config, "table1", tableacl.ADMIN, false); err != nil {
		t.Fatal(err)
	}
}

func testAllowUnmatchedTable(t *testing.T) {
	config := newConfigProto(
		"group01", []string{"table%"}, []string{currentUser}, []string{"u3"}, []string{})
	if err := checkAccess(config, "UNMATCHED_TABLE", tableacl.ADMIN, false); err != nil {
		t.Fatal(err)
	}
}

func newConfigProto(groupName string, tableNamesOrPrefixes, readers, writers, admins []string) *tableaclpb.Config {
	return &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 groupName,
			TableNamesOrPrefixes: tableNamesOrPrefixes,
			Readers:              readers,
			Writers:              writers,
			Admins:               admins,
		}},
	}
}

func checkLoad(config *tableaclpb.Config, valid bool) error {
	err := tableacl.InitFromProto(config)
	if !valid && err == nil {
		return errors.New("expecting parse error none returned")
	}

	if valid && err != nil {
		return fmt.Errorf("unexpected load error: %v", err)
	}
	return nil
}

func checkAccess(config *tableaclpb.Config, tableName string, role tableacl.Role, want bool) error {
	if err := checkLoad(config, true); err != nil {
		return err
	}
	got := tableacl.Authorized(tableName, role).IsMember(&querypb.VTGateCallerID{Username: currentUser})
	if want != got {
		return fmt.Errorf("got %v, want %v", got, want)
	}
	return nil
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
