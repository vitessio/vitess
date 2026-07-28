/*
Copyright 2019 The Vitess Authors.

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
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/tableacl/acl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tableaclpb "vitess.io/vitess/go/vt/proto/tableacl"
)

type fakeACLFactory struct{}

func (factory *fakeACLFactory) New(entries []string) (acl.ACL, error) {
	return nil, errors.New("unable to create a new ACL")
}

func TestInitWithInvalidFilePath(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	require.Error(t, tacl.init("/invalid_file_path", func() {}), "init should fail for an invalid config file path")
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
	f, err := os.CreateTemp("", "tableacl")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = io.WriteString(f, aclJSON)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, tacl.init(f.Name(), func() {}))
}

func TestInitWithEmptyConfig(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	f, err := os.CreateTemp("", "tableacl")
	require.NoError(t, err)

	defer os.Remove(f.Name())
	err = f.Close()
	require.NoError(t, err)

	err = tacl.init(f.Name(), func() {})
	require.Error(t, err)
}

func TestInitFromProto(t *testing.T) {
	tacl := tableACL{factory: &simpleacl.Factory{}}
	readerACL := tacl.Authorized("my_test_table", READER)
	want := &ACLResult{ACL: acl.DenyAllACL{}, GroupName: ""}
	require.Truef(t, reflect.DeepEqual(readerACL, want), "tableacl has not been initialized, got: %v, want: %v", readerACL, want)
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"vt"},
		}},
	}
	require.NoError(t, tacl.Set(config))
	got := tacl.Config()
	require.Truef(t, proto.Equal(got, config), "GetCurrentConfig() = %v, want: %v", got, config)
	readerACL = tacl.Authorized("unknown_table", READER)
	require.Truef(t, reflect.DeepEqual(readerACL, want), "there is no config for unknown_table, should deny by default")
	readerACL = tacl.Authorized("test_table", READER)
	require.True(t, readerACL.IsMember(&querypb.VTGateCallerID{Username: "vt"}), "user: vt should have reader permission to table: test_table")
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
		if test.valid {
			require.NoErrorf(t, err, "ValidateProto(%v)", config)
		} else {
			require.Errorf(t, err, "ValidateProto(%v) = nil, want error", config)
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
	require.NoError(t, tacl.Set(config))

	readerACL := tacl.Authorized("test_data_any", READER)
	require.True(t, readerACL.IsMember(&querypb.VTGateCallerID{Username: "u1"}), "user u1 should have reader permission to table test_data_any")
	require.True(t, readerACL.IsMember(&querypb.VTGateCallerID{Username: "u2"}), "user u2 should have reader permission to table test_data_any")
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
	require.Error(t, tacl.Set(config), "tableacl init should fail because fake ACL returns an error")
}

func TestDoubleRegisterTheSameKey(t *testing.T) {
	name := "tableacl-name-TestDoubleRegisterTheSameKey"
	Register(name, &simpleacl.Factory{})
	defer func() {
		err := recover()
		require.NotNil(t, err, "the second tableacl register should fail")
	}()
	Register(name, &simpleacl.Factory{})
}

func TestGetCurrentAclFactory(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := "tableacl-name-TestGetCurrentAclFactory"
	aclFactory := &simpleacl.Factory{}
	Register(name+"-1", aclFactory)
	f, err := GetCurrentACLFactory()
	require.NoError(t, err)
	require.Truef(t, reflect.DeepEqual(aclFactory, f), "should return registered acl factory even if default acl is not set.")
	Register(name+"-2", aclFactory)
	_, err = GetCurrentACLFactory()
	require.Error(t, err, "there are more than one acl factories, but the default is not set")
}

func TestGetCurrentACLFactoryWithWrongDefault(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := "tableacl-name-TestGetCurrentAclFactoryWithWrongDefault"
	aclFactory := &simpleacl.Factory{}
	Register(name+"-1", aclFactory)
	Register(name+"-2", aclFactory)
	SetDefaultACL("wrong_name")
	_, err := GetCurrentACLFactory()
	require.Error(t, err, "there are more than one acl factories, but the default given does not match any of these.")
}
