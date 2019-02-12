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
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"syscall"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
)

func TestRegisterFlagsWithSomeFlags(t *testing.T) {
	savedDBConfig := dbConfigs
	savedBaseConfig := baseConfig
	defer func() {
		dbConfigs = savedDBConfig
		baseConfig = savedBaseConfig
	}()

	dbConfigs = DBConfigs{userConfigs: make(map[string]*userConfig)}
	RegisterFlags(Dba, Repl)
	for k := range dbConfigs.userConfigs {
		if k != Dba && k != Repl {
			t.Errorf("dbConfigs.params: %v, want dba or repl", k)
		}
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
		userConfigs: map[string]*userConfig{
			App:      {param: mysql.ConnParams{UnixSocket: "socket"}},
			AppDebug: {},
			Dba:      {param: mysql.ConnParams{Host: "host"}},
		},
	}
	dbc, err := Init("default")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := dbc.userConfigs[App].param.UnixSocket, "socket"; got != want {
		t.Errorf("dbc.app.UnixSocket: %v, want %v", got, want)
	}
	if got, want := dbc.userConfigs[Dba].param.Host, "host"; got != want {
		t.Errorf("dbc.app.Host: %v, want %v", got, want)
	}
	if got, want := dbc.userConfigs[AppDebug].param.UnixSocket, "default"; got != want {
		t.Errorf("dbc.app.UnixSocket: %v, want %v", got, want)
	}

	baseConfig = mysql.ConnParams{
		Host:       "a",
		Port:       1,
		Uname:      "b",
		Pass:       "c",
		DbName:     "d",
		UnixSocket: "e",
		Charset:    "f",
		Flags:      2,
		SslCa:      "g",
		SslCaPath:  "h",
		SslCert:    "i",
		SslKey:     "j",
	}
	dbConfigs = DBConfigs{
		userConfigs: map[string]*userConfig{
			App: {
				param: mysql.ConnParams{
					Uname:      "app",
					Pass:       "apppass",
					UnixSocket: "socket",
				},
			},
			AppDebug: {
				useSSL: true,
			},
			Dba: {
				useSSL: true,
				param: mysql.ConnParams{
					Uname: "dba",
					Pass:  "dbapass",
					Host:  "host",
				},
			},
		},
	}
	dbc, err = Init("default")
	if err != nil {
		t.Fatal(err)
	}
	want := &DBConfigs{
		userConfigs: map[string]*userConfig{
			App: {
				param: mysql.ConnParams{
					Host:       "a",
					Port:       1,
					Uname:      "app",
					Pass:       "apppass",
					UnixSocket: "e",
					Charset:    "f",
					Flags:      2,
				},
			},
			AppDebug: {
				useSSL: true,
				param: mysql.ConnParams{
					Host:       "a",
					Port:       1,
					UnixSocket: "e",
					Charset:    "f",
					Flags:      2,
					SslCa:      "g",
					SslCaPath:  "h",
					SslCert:    "i",
					SslKey:     "j",
				},
			},
			Dba: {
				useSSL: true,
				param: mysql.ConnParams{
					Host:       "a",
					Port:       1,
					Uname:      "dba",
					Pass:       "dbapass",
					UnixSocket: "e",
					Charset:    "f",
					Flags:      2,
					SslCa:      "g",
					SslCaPath:  "h",
					SslCert:    "i",
					SslKey:     "j",
				},
			},
		},
	}
	// Compare individually, otherwise the errors are not readable.
	if !reflect.DeepEqual(dbc.userConfigs[App].param, want.userConfigs[App].param) {
		t.Errorf("dbc: \n%#v, want \n%#v", dbc.userConfigs[App].param, want.userConfigs[App].param)
	}
	if !reflect.DeepEqual(dbc.userConfigs[AppDebug].param, want.userConfigs[AppDebug].param) {
		t.Errorf("dbc: \n%#v, want \n%#v", dbc.userConfigs[AppDebug].param, want.userConfigs[AppDebug].param)
	}
	if !reflect.DeepEqual(dbc.userConfigs[Dba].param, want.userConfigs[Dba].param) {
		t.Errorf("dbc: \n%#v, want \n%#v", dbc.userConfigs[Dba].param, want.userConfigs[Dba].param)
	}
}

func TestAccessors(t *testing.T) {
	dbc := &DBConfigs{
		userConfigs: map[string]*userConfig{
			App:      {},
			AppDebug: {},
			AllPrivs: {},
			Dba:      {},
			Filtered: {},
			Repl:     {},
		},
	}
	dbc.DBName.Set("db")
	if got, want := dbc.AppWithDB().DbName, "db"; got != want {
		t.Errorf("dbc.AppWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.AllPrivsWithDB().DbName, "db"; got != want {
		t.Errorf("dbc.AllPrivsWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.AppDebugWithDB().DbName, "db"; got != want {
		t.Errorf("dbc.AppDebugWithDB().DbName: %v, want %v", got, want)
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

func TestCopy(t *testing.T) {
	want := &DBConfigs{
		userConfigs: map[string]*userConfig{
			App:      {param: mysql.ConnParams{UnixSocket: "aa"}},
			AppDebug: {},
			Repl:     {},
		},
	}
	want.DBName.Set("db")
	want.SidecarDBName.Set("_vt")

	got := want.Copy()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DBConfig: %v, want %v", got, want)
	}
}

func TestCredentialsFileHUP(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "credentials.json")
	if err != nil {
		t.Fatalf("couldn't create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	*dbCredentialsFile = tmpFile.Name()
	*dbCredentialsServer = "file"
	oldStr := "str1"
	jsonConfig := fmt.Sprintf("{\"%s\": [\"%s\"]}", oldStr, oldStr)
	if err := ioutil.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't write temp file: %v", err)
	}
	cs := GetCredentialsServer()
	_, pass, err := cs.GetUserAndPassword(oldStr)
	if pass != oldStr {
		t.Fatalf("%s's Password should still be '%s'", oldStr, oldStr)
	}
	hupTest(t, tmpFile, oldStr, "str2")
	hupTest(t, tmpFile, "str2", "str3") // still handling the signal
}

func hupTest(t *testing.T, tmpFile *os.File, oldStr, newStr string) {
	cs := GetCredentialsServer()
	jsonConfig := fmt.Sprintf("{\"%s\": [\"%s\"]}", newStr, newStr)
	if err := ioutil.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't overwrite temp file: %v", err)
	}
	_, pass, err := cs.GetUserAndPassword(oldStr)
	if pass != oldStr {
		t.Fatalf("%s's Password should still be '%s'", oldStr, oldStr)
	}
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond) // wait for signal handler
	_, pass, err = cs.GetUserAndPassword(oldStr)
	if err != ErrUnknownUser {
		t.Fatalf("Should not have old %s after config reload", oldStr)
	}
	_, pass, err = cs.GetUserAndPassword(newStr)
	if pass != newStr {
		t.Fatalf("%s's Password should be '%s'", newStr, newStr)
	}
}
