// Copyright 2023 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build linux

package mysql

import (
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
	"testing"
	"time"
)

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