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

package filebackupstorage

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"context"
)

// This file tests the file BackupStorage engine.

// Note this is a very generic test for BackupStorage implementations,
// we test the interface only. But making it a generic test library is
// more cumbersome, we'll do that when we have an actual need for
// another BackupStorage implementation.

// setupFileBackupStorage creates a temporary directory, and
// returns a FileBackupStorage based on it
func setupFileBackupStorage(t *testing.T) *FileBackupStorage {
	root, err := ioutil.TempDir("", "fbstest")
	if err != nil {
		t.Fatalf("os.TempDir failed: %v", err)
	}
	*FileBackupStorageRoot = root
	return &FileBackupStorage{}
}

// cleanupFileBackupStorage removes the entire directory
func cleanupFileBackupStorage(fbs *FileBackupStorage) {
	os.RemoveAll(*FileBackupStorageRoot)
}

func TestListBackups(t *testing.T) {
	fbs := setupFileBackupStorage(t)
	defer cleanupFileBackupStorage(fbs)
	ctx := context.Background()

	// verify we have no entry now
	dir := "keyspace/shard"
	bhs, err := fbs.ListBackups(ctx, dir)
	if err != nil {
		t.Fatalf("ListBackups on empty fbs failed: %v", err)
	}
	if len(bhs) != 0 {
		t.Fatalf("ListBackups on empty fbs returned results: %#v", bhs)
	}

	// add one empty backup
	firstBackup := "cell-0001-2015-01-14-10-00-00"
	bh, err := fbs.StartBackup(ctx, dir, firstBackup)
	if err != nil {
		t.Fatalf("fbs.StartBackup failed: %v", err)
	}
	if err := bh.EndBackup(ctx); err != nil {
		t.Fatalf("bh.EndBackup failed: %v", err)
	}

	// verify we have one entry now
	bhs, err = fbs.ListBackups(ctx, dir)
	if err != nil {
		t.Fatalf("ListBackups on empty fbs failed: %v", err)
	}
	if len(bhs) != 1 ||
		bhs[0].Directory() != dir ||
		bhs[0].Name() != firstBackup {
		t.Fatalf("ListBackups with one backup returned wrong results: %#v", bhs)
	}

	// add another one, with earlier date
	secondBackup := "cell-0001-2015-01-12-10-00-00"
	bh, err = fbs.StartBackup(ctx, dir, secondBackup)
	if err != nil {
		t.Fatalf("fbs.StartBackup failed: %v", err)
	}
	if err := bh.EndBackup(ctx); err != nil {
		t.Fatalf("bh.EndBackup failed: %v", err)
	}

	// verify we have two sorted entries now
	bhs, err = fbs.ListBackups(ctx, dir)
	if err != nil {
		t.Fatalf("ListBackups on empty fbs failed: %v", err)
	}
	if len(bhs) != 2 ||
		bhs[0].Directory() != dir ||
		bhs[0].Name() != secondBackup ||
		bhs[1].Directory() != dir ||
		bhs[1].Name() != firstBackup {
		t.Fatalf("ListBackups with two backups returned wrong results: %#v", bhs)
	}

	// remove a backup, back to one
	if err := fbs.RemoveBackup(ctx, dir, secondBackup); err != nil {
		t.Fatalf("RemoveBackup failed: %v", err)
	}
	bhs, err = fbs.ListBackups(ctx, dir)
	if err != nil {
		t.Fatalf("ListBackups after deletion failed: %v", err)
	}
	if len(bhs) != 1 ||
		bhs[0].Directory() != dir ||
		bhs[0].Name() != firstBackup {
		t.Fatalf("ListBackups after deletion returned wrong results: %#v", bhs)
	}

	// add a backup but abort it, should stay at one
	bh, err = fbs.StartBackup(ctx, dir, secondBackup)
	if err != nil {
		t.Fatalf("fbs.StartBackup failed: %v", err)
	}
	if err := bh.AbortBackup(ctx); err != nil {
		t.Fatalf("bh.AbortBackup failed: %v", err)
	}
	bhs, err = fbs.ListBackups(ctx, dir)
	if err != nil {
		t.Fatalf("ListBackups after abort failed: %v", err)
	}
	if len(bhs) != 1 ||
		bhs[0].Directory() != dir ||
		bhs[0].Name() != firstBackup {
		t.Fatalf("ListBackups after abort returned wrong results: %#v", bhs)
	}

	// check we cannot chaneg a backup we listed
	if _, err := bhs[0].AddFile(ctx, "test", 0); err == nil {
		t.Fatalf("was able to AddFile to read-only backup")
	}
	if err := bhs[0].EndBackup(ctx); err == nil {
		t.Fatalf("was able to EndBackup a read-only backup")
	}
	if err := bhs[0].AbortBackup(ctx); err == nil {
		t.Fatalf("was able to AbortBackup a read-only backup")
	}
}

func TestFileContents(t *testing.T) {
	fbs := setupFileBackupStorage(t)
	defer cleanupFileBackupStorage(fbs)
	ctx := context.Background()

	dir := "keyspace/shard"
	name := "cell-0001-2015-01-14-10-00-00"
	filename1 := "file1"
	contents1 := "contents of the first file"

	// start a backup, add a file
	bh, err := fbs.StartBackup(ctx, dir, name)
	if err != nil {
		t.Fatalf("fbs.StartBackup failed: %v", err)
	}
	wc, err := bh.AddFile(ctx, filename1, 0)
	if err != nil {
		t.Fatalf("bh.AddFile failed: %v", err)
	}
	if _, err := wc.Write([]byte(contents1)); err != nil {
		t.Fatalf("wc.Write failed: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("wc.Close failed: %v", err)
	}

	// test we can't read back on read-write backup
	if _, err := bh.ReadFile(ctx, filename1); err == nil {
		t.Fatalf("was able to ReadFile to read-write backup")
	}

	// and close
	if err := bh.EndBackup(ctx); err != nil {
		t.Fatalf("bh.EndBackup failed: %v", err)
	}

	// re-read the file
	bhs, err := fbs.ListBackups(ctx, dir)
	if err != nil || len(bhs) != 1 {
		t.Fatalf("ListBackups after abort returned wrong return: %v %v", err, bhs)
	}
	rc, err := bhs[0].ReadFile(ctx, filename1)
	if err != nil {
		t.Fatalf("bhs[0].ReadFile failed: %v", err)
	}
	buf := make([]byte, len(contents1)+10)
	if n, err := rc.Read(buf); (err != nil && err != io.EOF) || n != len(contents1) {
		t.Fatalf("rc.Read returned wrong result: %v %#v", err, n)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("rc.Close failed: %v", err)
	}
}
