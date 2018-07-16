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

package dbconfigs

import (
	"testing"

	"vitess.io/vitess/go/mysql"
)

func TestRegisterFlagsWithoutFlags(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("RegisterFlags should panic with empty db flags")
		}
	}()
	dbConfigs = DBConfigs{}
	RegisterFlags(EmptyConfig)
}

func TestRegisterFlagsWithSomeFlags(t *testing.T) {
	saved := dbFlags
	defer func() {
		dbFlags = saved
	}()

	dbConfigs = DBConfigs{}
	RegisterFlags(DbaConfig | ReplConfig)
	if dbFlags&AppConfig != 0 {
		t.Error("App connection params should not be registered.")
	}
	if dbFlags&DbaConfig == 0 {
		t.Error("Dba connection params should be registered.")
	}
	if dbFlags&FilteredConfig != 0 {
		t.Error("Filtered connection params should not be registered.")
	}
	if dbFlags&ReplConfig == 0 {
		t.Error("Repl connection params should be registered.")
	}
}

func TestInit(t *testing.T) {
	savedDBConfig := dbConfigs
	savedBaseConfig := baseConfig
	defer func() {
		dbConfigs = savedDBConfig
		baseConfig = savedBaseConfig
	}()

	dbConfigs = DBConfigs{
		app: mysql.ConnParams{
			UnixSocket: "socket",
		},
		dba: mysql.ConnParams{
			Host: "host",
		},
	}
	dbc, err := Init("default")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := dbc.app.UnixSocket, "socket"; got != want {
		t.Errorf("dbc.app.UnixSocket: %v, want %v", got, want)
	}
	if got, want := dbc.dba.Host, "host"; got != want {
		t.Errorf("dbc.app.Host: %v, want %v", got, want)
	}
	if got, want := dbc.appDebug.UnixSocket, "default"; got != want {
		t.Errorf("dbc.app.UnixSocket: %v, want %v", got, want)
	}

	baseConfig = mysql.ConnParams{
		Host: "basehost",
	}
	dbc, err = Init("default")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := dbc.app.UnixSocket, ""; got != want {
		t.Errorf("dbc.app.UnixSocket: %v, want %v", got, want)
	}
	if got, want := dbc.dba.Host, "basehost"; got != want {
		t.Errorf("dbc.app.Host: %v, want %v", got, want)
	}
	if got, want := dbc.appDebug.Host, "basehost"; got != want {
		t.Errorf("dbc.app.Host: %v, want %v", got, want)
	}
}

func TestAccessors(t *testing.T) {
	dbc := &DBConfigs{}
	dbc.DBName.Set("db")
	if got, want := dbc.AppWithDB().DbName, "db"; got != want {
		t.Errorf("dbc.AppWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.AppDebugWithDB().DbName, "db"; got != want {
		t.Errorf("dbc.AppDebugWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.AllPrivsWithDB().DbName, "db"; got != want {
		t.Errorf("dbc.AllPrivsWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.Dba().DbName, ""; got != want {
		t.Errorf("dbc.Dba().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.DbaWithDB().DbName, "db"; got != want {
		t.Errorf("dbc.DbaWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.FilteredWithDB().DbName, "db"; got != want {
		t.Errorf("dbc.FilteredWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.Repl().DbName, ""; got != want {
		t.Errorf("dbc.Repl().DbName: %v, want %v", got, want)
	}
}
