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

package tableacl

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/tableacl/acl"
	"github.com/youtube/vitess/go/vt/tableacl/simpleacl"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tableaclpb "github.com/youtube/vitess/go/vt/proto/tableacl"
)

type fakeACLFactory struct{}

func (factory *fakeACLFactory) New(entries []string) (acl.ACL, error) {
	return nil, errors.New("unable to create a new ACL")
}

func TestInitWithInvalidFilePath(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	if err := tacl.init("/invalid_file_path", func() {}); err == nil {
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
	tacl := tableACL{factory: &simpleacl.Factory{}}
	f, err := ioutil.TempFile("", "tableacl")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := io.WriteString(f, aclJSON); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tacl.init(f.Name(), func() {}); err != nil {
		t.Fatal(err)
	}
}

func TestInitFromProto(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	readerACL := tacl.Authorized("my_test_table", READER)
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
	if err := tacl.Set(config); err != nil {
		t.Fatalf("tableacl init should succeed, but got error: %v", err)
	}
	if got := tacl.Config(); !proto.Equal(got, config) {
		t.Fatalf("GetCurrentConfig() = %v, want: %v", got, config)
	}
	readerACL = tacl.Authorized("unknown_table", READER)
	if !reflect.DeepEqual(readerACL, want) {
		t.Fatalf("there is no config for unknown_table, should deny by default")
	}
	readerACL = tacl.Authorized("test_table", READER)
	if !readerACL.IsMember(&querypb.VTGateCallerID{Username: "vt"}) {
		t.Fatalf("user: vt should have reader permission to table: test_table")
	}
}

func TestTableACLValidateConfig(t *testing.T) {
	tests := []struct {
		names []string
		valid bool
	}{
		{nil, true},
		{[]string{}, true},
		{[]string{"b"}, true},
		{[]string{"b", "a"}, true},
		{[]string{"b%c"}, false},                    // invalid entry
		{[]string{"aaa", "aaab%", "aaabb"}, false},  // overlapping
		{[]string{"aaa", "aaab", "aaab%"}, false},   // overlapping
		{[]string{"a", "aa%", "aaab%"}, false},      // overlapping
		{[]string{"a", "aa%", "aaab"}, false},       // overlapping
		{[]string{"a", "aa", "aaa%%"}, false},       // invalid entry
		{[]string{"a", "aa", "aa", "aaaaa"}, false}, // duplicate
	}
	for _, test := range tests {
		var groups []*tableaclpb.TableGroupSpec
		for _, name := range test.names {
			groups = append(groups, &tableaclpb.TableGroupSpec{
				TableNamesOrPrefixes: []string{name},
			})
		}
		config := &tableaclpb.Config{TableGroups: groups}
		err := ValidateProto(config)
		if test.valid && err != nil {
			t.Fatalf("ValidateProto(%v) = %v, want nil", config, err)
		} else if !test.valid && err == nil {
			t.Fatalf("ValidateProto(%v) = nil, want error", config)
		}
	}
}

func TestTableACLAuthorize(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
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
	if err := tacl.Set(config); err != nil {
		t.Fatalf("InitFromProto(<data>) = %v, want: nil", err)
	}

	readerACL := tacl.Authorized("test_data_any", READER)
	if !readerACL.IsMember(&querypb.VTGateCallerID{Username: "u1"}) {
		t.Fatalf("user u1 should have reader permission to table test_data_any")
	}
	if !readerACL.IsMember(&querypb.VTGateCallerID{Username: "u2"}) {
		t.Fatalf("user u2 should have reader permission to table test_data_any")
	}
}

func TestFailedToCreateACL(t *testing.T) {
	tacl := tableACL{factory: &fakeACLFactory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"vt"},
			Writers:              []string{"vt"},
		}},
	}
	if err := tacl.Set(config); err == nil {
		t.Fatalf("tableacl init should fail because fake ACL returns an error")
	}
}

func TestDoubleRegisterTheSameKey(t *testing.T) {
	name := "tableacl-name-TestDoubleRegisterTheSameKey"
	Register(name, &simpleacl.Factory{})
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("the second tableacl register should fail")
		}
	}()
	Register(name, &simpleacl.Factory{})
}

func TestGetCurrentAclFactory(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := "tableacl-name-TestGetCurrentAclFactory"
	aclFactory := &simpleacl.Factory{}
	Register(name+"-1", aclFactory)
	f, err := GetCurrentAclFactory()
	if err != nil {
		t.Errorf("Fail to get current ACL Factory: %v", err)
	}
	if !reflect.DeepEqual(aclFactory, f) {
		t.Fatalf("should return registered acl factory even if default acl is not set.")
	}
	Register(name+"-2", aclFactory)
	_, err = GetCurrentAclFactory()
	if err == nil {
		t.Fatalf("there are more than one acl factories, but the default is not set")
	}
}

func TestGetCurrentAclFactoryWithWrongDefault(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := "tableacl-name-TestGetCurrentAclFactoryWithWrongDefault"
	aclFactory := &simpleacl.Factory{}
	Register(name+"-1", aclFactory)
	Register(name+"-2", aclFactory)
	SetDefaultACL("wrong_name")
	_, err := GetCurrentAclFactory()
	if err == nil {
		t.Fatalf("there are more than one acl factories, but the default given does not match any of these.")
	}
}

func TestHealthWithACL(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	ha := health.NewAggregator()
	tests := []struct {
		config *tableaclpb.Config
		wantOK bool
	}{
		{
			config: nil,
			wantOK: false,
		},
		{
			config: &tableaclpb.Config{
				TableGroups: []*tableaclpb.TableGroupSpec{{
					Name:                 "group01",
					TableNamesOrPrefixes: []string{"test_table"},
					Readers:              []string{"vt"},
					Writers:              []string{"vt"},
				}}},
			wantOK: true,
		},
		{
			config: &tableaclpb.Config{},
			wantOK: true,
		},
	}
	ha.RegisterSimpleCheck("tableacl", func() error { return checkHealth(&tacl) })
	for _, test := range tests {
		if test.config != nil {
			if err := tacl.Set(test.config); err != nil {
				t.Fatalf("tacl.Set(%#v) = %v, want %v", test.config, err, nil)
			}
		}
		delay, err := ha.Report(true, true)
		wantErr := error(nil)
		if !test.wantOK {
			wantErr = errors.New("<not nil>")
		}
		wantDelay := time.Duration(0)
		if delay != wantDelay || (err == nil) != test.wantOK {
			t.Errorf("ha.Report(true, true) == (%v, %v), want (%v, %v)", delay, err, wantDelay, wantErr)
		}
	}
}
