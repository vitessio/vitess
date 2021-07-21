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
	"io/ioutil"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
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
	err := parseConfig([]byte(jsonConfig), &config)
	if err != nil {
		t.Fatalf("should not get an error, but got: %v", err)
	}
	if len(config["mysql_user"]) != 1 {
		t.Fatalf("mysql_user config size should be equal to 1")
	}

	if len(config["mysql_user_2"]) != 1 {
		t.Fatalf("mysql_user config size should be equal to 1")
	}
	// works with new format
	jsonConfig = `{"mysql_user":[
		{"Password":"123", "UserData":"dummy", "SourceHost": "localhost"},
		{"Password": "123", "UserData": "mysql_user_all"},
		{"Password": "456", "UserData": "mysql_user_with_groups", "Groups": ["user_group"]}
	]}`
	err = parseConfig([]byte(jsonConfig), &config)
	if err != nil {
		t.Fatalf("should not get an error, but got: %v", err)
	}
	if len(config["mysql_user"]) != 3 {
		t.Fatalf("mysql_user config size should be equal to 3")
	}

	if config["mysql_user"][0].SourceHost != "localhost" {
		t.Fatalf("SourceHost should be equal to localhost")
	}

	if len(config["mysql_user"][2].Groups) != 1 || config["mysql_user"][2].Groups[0] != "user_group" {
		t.Fatalf("Groups should be equal to [\"user_group\"]")
	}

	jsonConfig = `{
		"mysql_user": [{"Password": "123", "UserData": "mysql_user_all", "InvalidKey": "oops"}]
	}`
	err = parseConfig([]byte(jsonConfig), &config)
	if err == nil {
		t.Fatalf("Invalid config should have errored, but didn't")
	}
}

func TestValidateHashGetter(t *testing.T) {
	jsonConfig := `{"mysql_user": [{"Password": "password", "UserData": "user.name", "Groups": ["user_group"]}]}`

	auth := NewAuthServerStatic("", jsonConfig, 0)
	defer auth.close()
	ip := net.ParseIP("127.0.0.1")
	addr := &net.IPAddr{IP: ip, Zone: ""}

	salt, err := NewSalt()
	if err != nil {
		t.Fatalf("error generating salt: %v", err)
	}

	scrambled := ScrambleMysqlNativePassword(salt, []byte("password"))
	getter, err := auth.ValidateHash(salt, "mysql_user", scrambled, addr)
	if err != nil {
		t.Fatalf("error validating password: %v", err)
	}

	callerID := getter.Get()
	if callerID.Username != "user.name" {
		t.Fatalf("getter username incorrect, expected \"user.name\", got %v", callerID.Username)
	}
	if len(callerID.Groups) != 1 || callerID.Groups[0] != "user_group" {
		t.Fatalf("getter groups incorrect, expected [\"user_group\"], got %v", callerID.Groups)
	}
}

func TestHostMatcher(t *testing.T) {
	ip := net.ParseIP("192.168.0.1")
	addr := &net.TCPAddr{IP: ip, Port: 9999}
	match := matchSourceHost(net.Addr(addr), "")
	if !match {
		t.Fatalf("Should match any address when target is empty")
	}

	match = matchSourceHost(net.Addr(addr), "localhost")
	if match {
		t.Fatalf("Should not match address when target is localhost")
	}

	socket := &net.UnixAddr{Name: "unixSocket", Net: "1"}
	match = matchSourceHost(net.Addr(socket), "localhost")
	if !match {
		t.Fatalf("Should match socket when target is localhost")
	}
}

func TestStaticConfigHUP(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "mysql_auth_server_static_file.json")
	if err != nil {
		t.Fatalf("couldn't create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	oldStr := "str5"
	jsonConfig := fmt.Sprintf("{\"%s\":[{\"Password\":\"%s\"}]}", oldStr, oldStr)
	if err := ioutil.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't write temp file: %v", err)
	}

	aStatic := NewAuthServerStatic(tmpFile.Name(), "", 0)
	defer aStatic.close()

	if aStatic.getEntries()[oldStr][0].Password != oldStr {
		t.Fatalf("%s's Password should still be '%s'", oldStr, oldStr)
	}

	hupTest(t, aStatic, tmpFile, oldStr, "str2")
	hupTest(t, aStatic, tmpFile, "str2", "str3") // still handling the signal

	// delete registered Auth server
	for auth := range authServers {
		delete(authServers, auth)
	}
}

func TestStaticConfigHUPWithRotation(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "mysql_auth_server_static_file.json")
	if err != nil {
		t.Fatalf("couldn't create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	oldStr := "str1"
	jsonConfig := fmt.Sprintf("{\"%s\":[{\"Password\":\"%s\"}]}", oldStr, oldStr)
	if err := ioutil.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't write temp file: %v", err)
	}

	aStatic := NewAuthServerStatic(tmpFile.Name(), "", 10*time.Millisecond)
	defer aStatic.close()

	if aStatic.getEntries()[oldStr][0].Password != oldStr {
		t.Fatalf("%s's Password should still be '%s'", oldStr, oldStr)
	}

	hupTestWithRotation(t, aStatic, tmpFile, oldStr, "str4")
	hupTestWithRotation(t, aStatic, tmpFile, "str4", "str5")
}

func hupTest(t *testing.T, aStatic *AuthServerStatic, tmpFile *os.File, oldStr, newStr string) {
	jsonConfig := fmt.Sprintf("{\"%s\":[{\"Password\":\"%s\"}]}", newStr, newStr)
	if err := ioutil.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't overwrite temp file: %v", err)
	}

	if aStatic.getEntries()[oldStr][0].Password != oldStr {
		t.Fatalf("%s's Password should still be '%s'", oldStr, oldStr)
	}

	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond) // wait for signal handler

	if aStatic.getEntries()[oldStr] != nil {
		t.Fatalf("Should not have old %s after config reload", oldStr)
	}
	if aStatic.getEntries()[newStr][0].Password != newStr {
		t.Fatalf("%s's Password should be '%s'", newStr, newStr)
	}
}

func hupTestWithRotation(t *testing.T, aStatic *AuthServerStatic, tmpFile *os.File, oldStr, newStr string) {
	jsonConfig := fmt.Sprintf("{\"%s\":[{\"Password\":\"%s\"}]}", newStr, newStr)
	if err := ioutil.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't overwrite temp file: %v", err)
	}

	time.Sleep(20 * time.Millisecond) // wait for signal handler

	if aStatic.getEntries()[oldStr] != nil {
		t.Fatalf("Should not have old %s after config reload", oldStr)
	}
	if aStatic.getEntries()[newStr][0].Password != newStr {
		t.Fatalf("%s's Password should be '%s'", newStr, newStr)
	}
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
			salt, err := NewSalt()
			if err != nil {
				t.Fatalf("error generating salt: %v", err)
			}

			scrambled := ScrambleMysqlNativePassword(salt, []byte(c.password))
			_, err = auth.ValidateHash(salt, c.user, scrambled, addr)

			if c.success {
				if err != nil {
					t.Fatalf("authentication should have succeeded: %v", err)
				}
			} else {
				if err == nil {
					t.Fatalf("authentication should have failed")
				}
			}
		})
	}
}
