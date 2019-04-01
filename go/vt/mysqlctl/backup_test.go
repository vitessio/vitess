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
	rocksdbDir := path.Join(dataDir, ".rocksdb")
	sdiOnlyDir := path.Join(dataDir, "sdi_dir")
	for _, s := range []string{innodbDataDir, innodbLogDir, dataDbDir, extraDir, outsideDbDir, rocksdbDir, sdiOnlyDir} {
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
	if err := ioutil.WriteFile(path.Join(rocksdbDir, "000011.sst"), []byte("rocksdb file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file 000011.sst: %v", err)
	}
	if err := ioutil.WriteFile(path.Join(sdiOnlyDir, "table1.sdi"), []byte("sdi file"), os.ModePerm); err != nil {
		t.Fatalf("failed to write file table1.sdi: %v", err)
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
			Name: ".rocksdb/000011.sst",
		},
		{
			Base: "Data",
			Name: "sdi_dir/table1.sdi",
		},
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
