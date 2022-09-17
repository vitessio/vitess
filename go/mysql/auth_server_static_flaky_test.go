/*
copyright 2019 The Vitess Authors.

licensed under the apache license, version 2.0 (the "license");
you may not use this file except in compliance with the license.
you may obtain a copy of the license at

    http://www.apache.org/licenses/license-2.0

unless required by applicable law or agreed to in writing, software
distributed under the license is distributed on an "as is" basis,
without warranties or conditions of any kind, either express or implied.
see the license for the specific language governing permissions and
limitations under the license.
*/

package mysql

import (
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// getEntries is a test-only method for AuthServerStatic.
func (a *AuthServerStatic) getEntries() map[string][]*AuthServerStaticEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.entries
}

func TestJsonConfigParser(t *testing.T) {
	// works with legacy format
	config := make(map[string][]*AuthServerStaticEntry)
	jsonConfig := "{\"mysql_user\":{\"Password\":\"123\", \"UserData\":\"dummy\"}, \"mysql_user_2\": {\"Password\": \"123\", \"UserData\": \"mysql_user_2\"}}"
	err := ParseConfig([]byte(jsonConfig), &config)
	require.NoError(t, err, "should not get an error, but got: %v", err)
	require.Equal(t, 1, len(config["mysql_user"]), "mysql_user config size should be equal to 1")
	require.Equal(t, 1, len(config["mysql_user_2"]), "mysql_user config size should be equal to 1")

	// works with new format
	jsonConfig = `{"mysql_user":[
		{"Password":"123", "UserData":"dummy", "SourceHost": "localhost"},
		{"Password": "123", "UserData": "mysql_user_all"},
		{"Password": "456", "UserData": "mysql_user_with_groups", "Groups": ["user_group"]}
	]}`
	err = ParseConfig([]byte(jsonConfig), &config)
	require.NoError(t, err, "should not get an error, but got: %v", err)
	require.Equal(t, 3, len(config["mysql_user"]), "mysql_user config size should be equal to 3")
	require.Equal(t, "localhost", config["mysql_user"][0].SourceHost, "SourceHost should be equal to localhost")

	if len(config["mysql_user"][2].Groups) != 1 || config["mysql_user"][2].Groups[0] != "user_group" {
		t.Fatalf("Groups should be equal to [\"user_group\"]")
	}

	jsonConfig = `{
		"mysql_user": [{"Password": "123", "UserData": "mysql_user_all", "InvalidKey": "oops"}]
	}`
	err = ParseConfig([]byte(jsonConfig), &config)
	require.Error(t, err, "Invalid config should have errored, but didn't")

}

func TestValidateHashGetter(t *testing.T) {
	jsonConfig := `{"mysql_user": [{"Password": "password", "UserData": "user.name", "Groups": ["user_group"]}]}`

	auth := NewAuthServerStatic("", jsonConfig, 0)
	defer auth.close()
	ip := net.ParseIP("127.0.0.1")
	addr := &net.IPAddr{IP: ip, Zone: ""}

	salt, err := newSalt()
	require.NoError(t, err, "error generating salt: %v", err)

	scrambled := ScrambleMysqlNativePassword(salt, []byte("password"))
	getter, err := auth.UserEntryWithHash(nil, salt, "mysql_user", scrambled, addr)
	require.NoError(t, err, "error validating password: %v", err)

	callerID := getter.Get()
	require.Equal(t, "user.name", callerID.Username, "getter username incorrect, expected \"user.name\", got %v", callerID.Username)

	if len(callerID.Groups) != 1 || callerID.Groups[0] != "user_group" {
		t.Fatalf("getter groups incorrect, expected [\"user_group\"], got %v", callerID.Groups)
	}
}

func TestHostMatcher(t *testing.T) {
	ip := net.ParseIP("192.168.0.1")
	addr := &net.TCPAddr{IP: ip, Port: 9999}
	match := MatchSourceHost(net.Addr(addr), "")
	require.True(t, match, "Should match any address when target is empty")

	match = MatchSourceHost(net.Addr(addr), "localhost")
	require.False(t, match, "Should not match address when target is localhost")

	socket := &net.UnixAddr{Name: "unixSocket", Net: "1"}
	match = MatchSourceHost(net.Addr(socket), "localhost")
	require.True(t, match, "Should match socket when target is localhost")

}

func TestStaticConfigHUP(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "mysql_auth_server_static_file.json")
	require.NoError(t, err, "couldn't create temp file: %v", err)

	defer os.Remove(tmpFile.Name())

	oldStr := "str5"
	jsonConfig := fmt.Sprintf("{\"%s\":[{\"Password\":\"%s\"}]}", oldStr, oldStr)
	if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't write temp file: %v", err)
	}

	aStatic := NewAuthServerStatic(tmpFile.Name(), "", 0)
	defer aStatic.close()
	require.Equal(t, oldStr, aStatic.getEntries()[oldStr][0].Password, "%s's Password should still be '%s'", oldStr, oldStr)

	hupTest(t, aStatic, tmpFile, oldStr, "str2")
	hupTest(t, aStatic, tmpFile, "str2", "str3") // still handling the signal

	mu.Lock()
	defer mu.Unlock()
	// delete registered Auth server
	for auth := range authServers {
		delete(authServers, auth)
	}
}

func TestStaticConfigHUPWithRotation(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "mysql_auth_server_static_file.json")
	require.NoError(t, err, "couldn't create temp file: %v", err)

	defer os.Remove(tmpFile.Name())

	oldStr := "str1"
	jsonConfig := fmt.Sprintf("{\"%s\":[{\"Password\":\"%s\"}]}", oldStr, oldStr)
	if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't write temp file: %v", err)
	}

	aStatic := NewAuthServerStatic(tmpFile.Name(), "", 10*time.Millisecond)
	defer aStatic.close()
	require.Equal(t, oldStr, aStatic.getEntries()[oldStr][0].Password, "%s's Password should still be '%s'", oldStr, oldStr)

	hupTestWithRotation(t, aStatic, tmpFile, oldStr, "str4")
	hupTestWithRotation(t, aStatic, tmpFile, "str4", "str5")
}

func hupTest(t *testing.T, aStatic *AuthServerStatic, tmpFile *os.File, oldStr, newStr string) {
	jsonConfig := fmt.Sprintf("{\"%s\":[{\"Password\":\"%s\"}]}", newStr, newStr)
	if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't overwrite temp file: %v", err)
	}
	require.Equal(t, oldStr, aStatic.getEntries()[oldStr][0].Password, "%s's Password should still be '%s'", oldStr, oldStr)

	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond)
	require. // wait for signal handler
			Nil(t, aStatic.getEntries()[oldStr], "Should not have old %s after config reload", oldStr)
	require.Equal(t, newStr, aStatic.getEntries()[newStr][0].Password, "%s's Password should be '%s'", newStr, newStr)

}

func hupTestWithRotation(t *testing.T, aStatic *AuthServerStatic, tmpFile *os.File, oldStr, newStr string) {
	jsonConfig := fmt.Sprintf("{\"%s\":[{\"Password\":\"%s\"}]}", newStr, newStr)
	if err := os.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't overwrite temp file: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	require. // wait for signal handler
			Nil(t, aStatic.getEntries()[oldStr], "Should not have old %s after config reload", oldStr)
	require.Equal(t, newStr, aStatic.getEntries()[newStr][0].Password, "%s's Password should be '%s'", newStr, newStr)

}

func TestStaticPasswords(t *testing.T) {
	jsonConfig := `
{
	"user01": [{ "Password": "user01" }],
	"user02": [{
		"MysqlNativePassword": "*B3AD996B12F211BEA47A7C666CC136FB26DC96AF"
	}],
	"user03": [{
		"MysqlNativePassword": "*211E0153B172BAED4352D5E4628BD76731AF83E7",
		"Password": "invalid"
	}],
	"user04": [
		{ "MysqlNativePassword": "*668425423DB5193AF921380129F465A6425216D0" },
		{ "Password": "password2" }
	]
}`

	tests := []struct {
		user     string
		password string
		success  bool
	}{
		{"user01", "user01", true},
		{"user01", "password", false},
		{"user01", "", false},
		{"user02", "user02", true},
		{"user02", "password", false},
		{"user02", "", false},
		{"user03", "user03", true},
		{"user03", "password", false},
		{"user03", "invalid", false},
		{"user03", "", false},
		{"user04", "password1", true},
		{"user04", "password2", true},
		{"user04", "", false},
		{"userXX", "", false},
		{"userXX", "", false},
		{"", "", false},
		{"", "password", false},
	}

	auth := NewAuthServerStatic("", jsonConfig, 0)
	defer auth.close()
	ip := net.ParseIP("127.0.0.1")
	addr := &net.IPAddr{IP: ip, Zone: ""}

	for _, c := range tests {
		t.Run(fmt.Sprintf("%s-%s", c.user, c.password), func(t *testing.T) {
			salt, err := newSalt()
			require.NoError(t, err, "error generating salt: %v", err)

			scrambled := ScrambleMysqlNativePassword(salt, []byte(c.password))
			_, err = auth.UserEntryWithHash(nil, salt, c.user, scrambled, addr)

			if c.success {
				require.NoError(t, err, "authentication should have succeeded: %v", err)

			} else {
				require.Error(t, err, "authentication should have failed")

			}
		})
	}
}
