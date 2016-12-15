// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sort"
	"testing"
)

func TestFindFilesToBackup(t *testing.T) {
	root, err := ioutil.TempDir("", "backuptest")
	if err != nil {
		t.Fatalf("os.TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)

	// Initialize the fake mysql root directories
	innodbDataDir := path.Join(root, "innodb_data")
	innodbLogDir := path.Join(root, "innodb_log")
	dataDir := path.Join(root, "data")
	dataDbDir := path.Join(dataDir, "vt_db")
	extraDir := path.Join(dataDir, "extra_dir")
	outsideDbDir := path.Join(root, "outside_db")
	for _, s := range []string{innodbDataDir, innodbLogDir, dataDbDir, extraDir, outsideDbDir} {
		if err := os.MkdirAll(s, os.ModePerm); err != nil {
			t.Fatalf("failed to create directory %v: %v", s, err)
		}
	}
	if err := ioutil.WriteFile(path.Join(innodbDataDir, "innodb_data_1"), []byte("innodb data 1 contents"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file innodb_data_1: %v", err)
	}
	if err := ioutil.WriteFile(path.Join(innodbLogDir, "innodb_log_1"), []byte("innodb log 1 contents"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file innodb_log_1: %v", err)
	}
	if err := ioutil.WriteFile(path.Join(dataDbDir, "db.opt"), []byte("db opt file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file db.opt: %v", err)
	}
	if err := ioutil.WriteFile(path.Join(extraDir, "extra.stuff"), []byte("extra file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file extra.stuff: %v", err)
	}
	if err := ioutil.WriteFile(path.Join(outsideDbDir, "table1.frm"), []byte("frm file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file table1.opt: %v", err)
	}
	if err := os.Symlink(outsideDbDir, path.Join(dataDir, "vt_symlink")); err != nil {
		t.Fatalf("failed to symlink vt_symlink: %v", err)
	}

	cnf := &Mycnf{
		InnodbDataHomeDir:     innodbDataDir,
		InnodbLogGroupHomeDir: innodbLogDir,
		DataDir:               dataDir,
	}

	result, err := findFilesToBackup(cnf)
	if err != nil {
		t.Fatalf("findFilesToBackup failed: %v", err)
	}
	sort.Sort(forTest(result))
	t.Logf("findFilesToBackup returned: %v", result)
	expected := []FileEntry{
		{
			Base: "Data",
			Name: "vt_db/db.opt",
		},
		{
			Base: "Data",
			Name: "vt_symlink/table1.frm",
		},
		{
			Base: "InnoDBData",
			Name: "innodb_data_1",
		},
		{
			Base: "InnoDBLog",
			Name: "innodb_log_1",
		},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("got wrong list of FileEntry %v, expected %v", result, expected)
	}
}

type forTest []FileEntry

func (f forTest) Len() int           { return len(f) }
func (f forTest) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f forTest) Less(i, j int) bool { return f[i].Base+f[i].Name < f[j].Base+f[j].Name }
