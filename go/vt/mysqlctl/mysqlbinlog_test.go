// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	log "github.com/golang/glog"
)

var (
	rootName = "VT_MYSQL_ROOT"
	rootPath = path.Join(os.TempDir(), "binlogtest")
)

func setup(cmd string, exitCode int) (env string) {
	env = os.Getenv(rootName)
	os.Setenv(rootName, rootPath)
	err := os.Mkdir(rootPath, 0755)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(path.Join(rootPath, "bin"), 0755)
	if err != nil {
		panic(err)
	}
	f, err := os.OpenFile(path.Join(rootPath, "bin/mysqlbinlog"), os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(f, "#!/bin/bash\n%s\nexit %d", cmd, exitCode)
	f.Close()
	return
}

func cleanup(env string) {
	os.Setenv(rootName, env)
	os.Remove(path.Join(rootPath, "bin/mysqlbinlog"))
	os.Remove(path.Join(rootPath, "bin"))
	os.Remove(rootPath)
}

func TestSuccess(t *testing.T) {
	env := setup("echo success $*", 0)
	defer cleanup(env)

	mbl := &MysqlBinlog{}
	out, err := mbl.Launch("name", 10)
	if err != nil {
		panic(err)
	}
	outbytes, err := ioutil.ReadAll(out)
	if err != nil {
		panic(err)
	}
	got := string(outbytes)
	want := "success --start-position=10 name\n"
	if want != got {
		t.Errorf("want '%s', got '%s'", want, got)
	}
}

func TestLaunchFail(t *testing.T) {
	env := setup("echo success $*", 0)
	defer cleanup(env)

	err := os.Chmod(path.Join(rootPath, "bin/mysqlbinlog"), 0644)
	if err != nil {
		panic(err)
	}

	mbl := &MysqlBinlog{}
	_, err = mbl.Launch("name", 10)
	if err == nil {
		t.Errorf("want error, got nil")
	}
}

func TestExitFail(t *testing.T) {
	env := setup("echo success $*", 1)
	defer cleanup(env)

	mbl := &MysqlBinlog{}
	out, err := mbl.Launch("name", 10)
	if err != nil {
		panic(err)
	}
	ioutil.ReadAll(out)
	err = mbl.Wait()
	want := "exit status 1"
	if want != err.Error() {
		t.Errorf("want %s, got %v", want, err.Error())
	}
}

func TestWarning(t *testing.T) {
	env := setup("echo WARNING $* 1>&2", 1)
	defer cleanup(env)

	mbl := &MysqlBinlog{}
	out, err := mbl.Launch("name", 10)
	if err != nil {
		panic(err)
	}
	ioutil.ReadAll(out)
	mbl.Wait()
	log.Flush()
	warnbytes, err := ioutil.ReadFile(path.Join(os.TempDir(), "mysqlctl.test.WARNING"))
	if err != nil {
		t.Error(err)
	}
	got := string(warnbytes)
	want := "WARNING --start-position=10 name"
	if !strings.Contains(got, want) {
		t.Errorf("want '%s' in '%s'", want, got)
	}
}

func TestError(t *testing.T) {
	env := setup("echo ERROR test: expected $* 1>&2", 1)
	defer cleanup(env)

	mbl := &MysqlBinlog{}
	out, err := mbl.Launch("name", 10)
	if err != nil {
		panic(err)
	}
	ioutil.ReadAll(out)
	mbl.Wait()
	log.Flush()
	warnbytes, err := ioutil.ReadFile(path.Join(os.TempDir(), "mysqlctl.test.ERROR"))
	if err != nil {
		t.Error(err)
	}
	got := string(warnbytes)
	want := "ERROR test: expected --start-position=10 name"
	if !strings.Contains(got, want) {
		t.Errorf("want '%s' in '%s'", want, got)
	}
}
