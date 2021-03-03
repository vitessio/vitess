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

package dbconfigs

import (
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/yaml2"
)

func TestInit(t *testing.T) {
	dbConfigs := DBConfigs{
		appParams: mysql.ConnParams{UnixSocket: "socket"},
		dbaParams: mysql.ConnParams{Host: "host"},
	}
	dbConfigs.InitWithSocket("default")
	assert.Equal(t, mysql.ConnParams{UnixSocket: "socket"}, dbConfigs.appParams)
	assert.Equal(t, mysql.ConnParams{Host: "host"}, dbConfigs.dbaParams)
	assert.Equal(t, mysql.ConnParams{UnixSocket: "default"}, dbConfigs.appdebugParams)

	dbConfigs = DBConfigs{
		Host:                       "a",
		Port:                       1,
		Socket:                     "b",
		Charset:                    "c",
		Flags:                      2,
		Flavor:                     "flavor",
		SslCa:                      "d",
		SslCaPath:                  "e",
		SslCert:                    "f",
		SslKey:                     "g",
		ConnectTimeoutMilliseconds: 250,
		App: UserConfig{
			User:     "app",
			Password: "apppass",
		},
		Appdebug: UserConfig{
			UseSSL: true,
		},
		Dba: UserConfig{
			User:     "dba",
			Password: "dbapass",
			UseSSL:   true,
		},
		appParams: mysql.ConnParams{
			UnixSocket: "socket",
		},
		dbaParams: mysql.ConnParams{
			Host: "host",
		},
	}
	dbConfigs.InitWithSocket("default")

	want := mysql.ConnParams{
		Host:             "a",
		Port:             1,
		Uname:            "app",
		Pass:             "apppass",
		UnixSocket:       "b",
		Charset:          "c",
		Flags:            2,
		Flavor:           "flavor",
		ConnectTimeoutMs: 250,
	}
	assert.Equal(t, want, dbConfigs.appParams)

	want = mysql.ConnParams{
		Host:             "a",
		Port:             1,
		UnixSocket:       "b",
		Charset:          "c",
		Flags:            2,
		Flavor:           "flavor",
		SslCa:            "d",
		SslCaPath:        "e",
		SslCert:          "f",
		SslKey:           "g",
		ConnectTimeoutMs: 250,
	}
	assert.Equal(t, want, dbConfigs.appdebugParams)
	want = mysql.ConnParams{
		Host:             "a",
		Port:             1,
		Uname:            "dba",
		Pass:             "dbapass",
		UnixSocket:       "b",
		Charset:          "c",
		Flags:            2,
		Flavor:           "flavor",
		SslCa:            "d",
		SslCaPath:        "e",
		SslCert:          "f",
		SslKey:           "g",
		ConnectTimeoutMs: 250,
	}
	assert.Equal(t, want, dbConfigs.dbaParams)

	// Test that baseConfig does not override Charset and Flag if they're
	// not specified.
	dbConfigs = DBConfigs{
		Host:      "a",
		Port:      1,
		Socket:    "b",
		SslCa:     "d",
		SslCaPath: "e",
		SslCert:   "f",
		SslKey:    "g",
		App: UserConfig{
			User:     "app",
			Password: "apppass",
		},
		Appdebug: UserConfig{
			UseSSL: true,
		},
		Dba: UserConfig{
			User:     "dba",
			Password: "dbapass",
			UseSSL:   true,
		},
		appParams: mysql.ConnParams{
			UnixSocket: "socket",
			Charset:    "f",
		},
		dbaParams: mysql.ConnParams{
			Host:  "host",
			Flags: 2,
		},
	}
	dbConfigs.InitWithSocket("default")
	want = mysql.ConnParams{
		Host:       "a",
		Port:       1,
		Uname:      "app",
		Pass:       "apppass",
		UnixSocket: "b",
		Charset:    "f",
	}
	assert.Equal(t, want, dbConfigs.appParams)
	want = mysql.ConnParams{
		Host:       "a",
		Port:       1,
		UnixSocket: "b",
		SslCa:      "d",
		SslCaPath:  "e",
		SslCert:    "f",
		SslKey:     "g",
	}
	assert.Equal(t, want, dbConfigs.appdebugParams)
	want = mysql.ConnParams{
		Host:       "a",
		Port:       1,
		Uname:      "dba",
		Pass:       "dbapass",
		UnixSocket: "b",
		Flags:      2,
		SslCa:      "d",
		SslCaPath:  "e",
		SslCert:    "f",
		SslKey:     "g",
	}
	assert.Equal(t, want, dbConfigs.dbaParams)
}

func TestUseTCP(t *testing.T) {
	dbConfigs := DBConfigs{
		Host:   "a",
		Port:   1,
		Socket: "b",
		App: UserConfig{
			User:   "app",
			UseTCP: true,
		},
		Dba: UserConfig{
			User: "dba",
		},
	}
	dbConfigs.InitWithSocket("default")

	want := mysql.ConnParams{
		Host:  "a",
		Port:  1,
		Uname: "app",
	}
	assert.Equal(t, want, dbConfigs.appParams)

	want = mysql.ConnParams{
		Host:       "a",
		Port:       1,
		Uname:      "dba",
		UnixSocket: "b",
	}
	assert.Equal(t, want, dbConfigs.dbaParams)
}

func TestAccessors(t *testing.T) {
	dbc := &DBConfigs{
		appParams:      mysql.ConnParams{},
		appdebugParams: mysql.ConnParams{},
		allprivsParams: mysql.ConnParams{},
		dbaParams:      mysql.ConnParams{},
		filteredParams: mysql.ConnParams{},
		replParams:     mysql.ConnParams{},
		DBName:         "db",
	}
	if got, want := dbc.AppWithDB().connParams.DbName, "db"; got != want {
		t.Errorf("dbc.AppWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.AllPrivsConnector().connParams.DbName, ""; got != want {
		t.Errorf("dbc.AllPrivsWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.AllPrivsWithDB().connParams.DbName, "db"; got != want {
		t.Errorf("dbc.AllPrivsWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.AppDebugWithDB().connParams.DbName, "db"; got != want {
		t.Errorf("dbc.AppDebugWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.DbaConnector().connParams.DbName, ""; got != want {
		t.Errorf("dbc.Dba().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.DbaWithDB().connParams.DbName, "db"; got != want {
		t.Errorf("dbc.DbaWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.FilteredWithDB().connParams.DbName, "db"; got != want {
		t.Errorf("dbc.FilteredWithDB().DbName: %v, want %v", got, want)
	}
	if got, want := dbc.ReplConnector().connParams.DbName, ""; got != want {
		t.Errorf("dbc.Repl().DbName: %v, want %v", got, want)
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
	_, pass, _ := cs.GetUserAndPassword(oldStr)
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
	_, pass, _ := cs.GetUserAndPassword(oldStr)
	if pass != oldStr {
		t.Fatalf("%s's Password should still be '%s'", oldStr, oldStr)
	}
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond) // wait for signal handler
	_, _, err := cs.GetUserAndPassword(oldStr)
	if err != ErrUnknownUser {
		t.Fatalf("Should not have old %s after config reload", oldStr)
	}
	_, pass, _ = cs.GetUserAndPassword(newStr)
	if pass != newStr {
		t.Fatalf("%s's Password should be '%s'", newStr, newStr)
	}
}

func TestYaml(t *testing.T) {
	db := DBConfigs{
		Socket: "a",
		Port:   1,
		Flags:  20,
		App: UserConfig{
			User:   "vt_app",
			UseSSL: true,
		},
		Dba: UserConfig{
			User: "vt_dba",
		},
	}
	gotBytes, err := yaml2.Marshal(&db)
	require.NoError(t, err)
	wantBytes := `allprivs:
  password: '****'
app:
  password: '****'
  useSsl: true
  user: vt_app
appdebug:
  password: '****'
dba:
  password: '****'
  user: vt_dba
filtered:
  password: '****'
flags: 20
port: 1
repl:
  password: '****'
socket: a
`
	assert.Equal(t, wantBytes, string(gotBytes))

	inBytes := []byte(`socket: a
port: 1
flags: 20
app:
  user: vt_app
  useSsl: true
  useTCP: false
dba:
  user: vt_dba
`)
	gotdb := DBConfigs{
		Port:  1,
		Flags: 20,
		App: UserConfig{
			UseTCP: true,
		},
		Dba: UserConfig{
			User: "aaa",
		},
	}
	err = yaml2.Unmarshal(inBytes, &gotdb)
	require.NoError(t, err)
	assert.Equal(t, &db, &gotdb)
}
