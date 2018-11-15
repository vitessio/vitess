/*
copyright 2017 google inc.

licensed under the apache license, version 2.0 (the "license");
you may not use this file except in compliance with the license.
you may obtain a copy of the license at

    http://www.apache.org/licenses/license-2.0

unless required by applicable law or agreedto in writing, software
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
	jsonConfig = "{\"mysql_user\":[{\"Password\":\"123\", \"UserData\":\"dummy\", \"SourceHost\": \"localhost\"}, {\"Password\": \"123\", \"UserData\": \"mysql_user_all\"}]}"
	err = parseConfig([]byte(jsonConfig), &config)
	if err != nil {
		t.Fatalf("should not get an error, but got: %v", err)
	}
	if len(config["mysql_user"]) != 2 {
		t.Fatalf("mysql_user config size should be equal to 2")
	}

	if config["mysql_user"][0].SourceHost != "localhost" {
		t.Fatalf("SourceHost should be equal localhost")
	}
}

func TestHostMatcher(t *testing.T) {
	ip := net.ParseIP("192.168.0.1")
	addr := &net.TCPAddr{IP: ip, Port: 9999}
	match := matchSourceHost(net.Addr(addr), "")
	if !match {
		t.Fatalf("Should match any addres when target is empty")
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
	*mysqlAuthServerStaticFile = tmpFile.Name()

	oldStr := "str1"
	jsonConfig := fmt.Sprintf("{\"%s\":[{\"Password\":\"%s\"}]}", oldStr, oldStr)
	if err := ioutil.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't write temp file: %v", err)
	}

	InitAuthServerStatic()
	aStatic := GetAuthServer("static").(*AuthServerStatic)

	if aStatic.Entries[oldStr][0].Password != oldStr {
		t.Fatalf("%s's Password should still be '%s'", oldStr, oldStr)
	}

	hupTest(t, tmpFile, oldStr, "str2")
	hupTest(t, tmpFile, "str2", "str3") // still handling the signal
}

func hupTest(t *testing.T, tmpFile *os.File, oldStr, newStr string) {
	aStatic := GetAuthServer("static").(*AuthServerStatic)

	jsonConfig := fmt.Sprintf("{\"%s\":[{\"Password\":\"%s\"}]}", newStr, newStr)
	if err := ioutil.WriteFile(tmpFile.Name(), []byte(jsonConfig), 0600); err != nil {
		t.Fatalf("couldn't overwrite temp file: %v", err)
	}

	if aStatic.Entries[oldStr][0].Password != oldStr {
		t.Fatalf("%s's Password should still be '%s'", oldStr, oldStr)
	}

	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	time.Sleep(100 * time.Millisecond) // wait for signal handler

	if aStatic.Entries[oldStr] != nil {
		t.Fatalf("Should not have old %s after config reload", oldStr)
	}
	if aStatic.Entries[newStr][0].Password != newStr {
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

	auth := NewAuthServerStatic()
	auth.loadConfigFromParams("", jsonConfig)
	ip := net.ParseIP("127.0.0.1")
	addr := &net.IPAddr{ip, ""}

	for _, c := range tests {
		t.Run(fmt.Sprintf("%s-%s", c.user, c.password), func(t *testing.T) {
			salt, err := NewSalt()
			if err != nil {
				t.Fatalf("error generating salt: %v", err)
			}

			scrambled := ScramblePassword(salt, []byte(c.password))
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
