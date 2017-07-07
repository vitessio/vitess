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

package mysqlctl

import (
	"os"
	"testing"

	"github.com/youtube/vitess/go/mysql"
)

type fakeMysqlFlavor string

func (f fakeMysqlFlavor) VersionMatch(version string) bool         { return version == string(f) }
func (fakeMysqlFlavor) ParseGTID(string) (mysql.GTID, error)       { return nil, nil }
func (fakeMysqlFlavor) EnableBinlogPlayback(mysqld *Mysqld) error  { return nil }
func (fakeMysqlFlavor) DisableBinlogPlayback(mysqld *Mysqld) error { return nil }

func TestMysqlFlavorEnvironmentVariable(t *testing.T) {
	os.Setenv("MYSQL_FLAVOR", "fake flavor")
	mysqlFlavors = make(map[string]MysqlFlavor)
	mysqlFlavors["fake flavor"] = fakeMysqlFlavor("fake flavor")
	mysqlFlavors["it's a trap"] = fakeMysqlFlavor("it's a trap")
	want := mysqlFlavors["fake flavor"]

	got, err := ((*Mysqld)(nil)).detectFlavor()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("detectFlavor() = %#v, want %#v", got, want)
	}
}

func TestRegisterFlavorBuiltin(t *testing.T) {
	registerFlavorBuiltin("TestRegisterFlavorBuiltin", fakeMysqlFlavor("builtin"))

	want := fakeMysqlFlavor("builtin")
	if got := mysqlFlavors["TestRegisterFlavorBuiltin"]; got != want {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestRegisterFlavorOverrideFirst(t *testing.T) {
	registerFlavorOverride("TestRegisterFlavorOverrideFirst", fakeMysqlFlavor("override"))
	registerFlavorBuiltin("TestRegisterFlavorOverrideFirst", fakeMysqlFlavor("builtin"))

	want := fakeMysqlFlavor("override")
	if got := mysqlFlavors["TestRegisterFlavorOverrideFirst"]; got != want {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestRegisterFlavorOverrideSecond(t *testing.T) {
	registerFlavorBuiltin("TestRegisterFlavorOverrideSecond", fakeMysqlFlavor("builtin"))
	registerFlavorOverride("TestRegisterFlavorOverrideSecond", fakeMysqlFlavor("override"))

	want := fakeMysqlFlavor("override")
	if got := mysqlFlavors["TestRegisterFlavorOverrideSecond"]; got != want {
		t.Errorf("got %#v, want %#v", got, want)
	}
}
