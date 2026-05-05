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
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

// This file tests the file BackupStorage engine.

// Note this is a very generic test for BackupStorage implementations,
// we test the interface only. But making it a generic test library is
// more cumbersome, we'll do that when we have an actual need for
// another BackupStorage implementation.

// setupFileBackupStorage creates a temporary directory, and
// returns a FileBackupStorage based on it
func setupFileBackupStorage(t *testing.T) backupstorage.BackupStorage {
	FileBackupStorageRoot = t.TempDir()
	return newFileBackupStorage(backupstorage.NoParams())
}

func TestListBackups(t *testing.T) {
	fbs := setupFileBackupStorage(t)
	ctx := context.Background()

	// verify we have no entry now
	dir := "keyspace/shard"
	bhs, err := fbs.ListBackups(ctx, dir)
	require.NoError(t, err)
	require.Emptyf(t, bhs, "ListBackups on empty fbs returned results: %#v", bhs)

	// add one empty backup
	firstBackup := "cell-0001-2015-01-14-10-00-00"
	bh, err := fbs.StartBackup(ctx, dir, firstBackup)
	require.NoError(t, err)
	require.NoError(t, bh.EndBackup(ctx))

	// verify we have one entry now
	bhs, err = fbs.ListBackups(ctx, dir)
	require.NoError(t, err)
	require.Truef(t,
		len(bhs) == 1 && bhs[0].Directory() == dir && bhs[0].Name() == firstBackup,
		"ListBackups with one backup returned wrong results: %#v", bhs)

	// add another one, with earlier date
	secondBackup := "cell-0001-2015-01-12-10-00-00"
	bh, err = fbs.StartBackup(ctx, dir, secondBackup)
	require.NoError(t, err)
	require.NoError(t, bh.EndBackup(ctx))

	// verify we have two sorted entries now
	bhs, err = fbs.ListBackups(ctx, dir)
	require.NoError(t, err)
	require.Truef(t,
		len(bhs) == 2 &&
			bhs[0].Directory() == dir && bhs[0].Name() == secondBackup &&
			bhs[1].Directory() == dir && bhs[1].Name() == firstBackup,
		"ListBackups with two backups returned wrong results: %#v", bhs)

	// remove a backup, back to one
	require.NoError(t, fbs.RemoveBackup(ctx, dir, secondBackup))
	bhs, err = fbs.ListBackups(ctx, dir)
	require.NoError(t, err)
	require.Truef(t,
		len(bhs) == 1 && bhs[0].Directory() == dir && bhs[0].Name() == firstBackup,
		"ListBackups after deletion returned wrong results: %#v", bhs)

	// add a backup but abort it, should stay at one
	bh, err = fbs.StartBackup(ctx, dir, secondBackup)
	require.NoError(t, err)
	require.NoError(t, bh.AbortBackup(ctx))
	bhs, err = fbs.ListBackups(ctx, dir)
	require.NoError(t, err)
	require.Truef(t,
		len(bhs) == 1 && bhs[0].Directory() == dir && bhs[0].Name() == firstBackup,
		"ListBackups after abort returned wrong results: %#v", bhs)

	// check we cannot change a backup we listed
	_, err = bhs[0].AddFile(ctx, "test", 0)
	require.Error(t, err, "was able to AddFile to read-only backup")
	require.Error(t, bhs[0].EndBackup(ctx), "was able to EndBackup a read-only backup")
	require.Error(t, bhs[0].AbortBackup(ctx), "was able to AbortBackup a read-only backup")
}

func TestFileContents(t *testing.T) {
	fbs := setupFileBackupStorage(t)
	ctx := context.Background()

	dir := "keyspace/shard"
	name := "cell-0001-2015-01-14-10-00-00"
	filename1 := "file1"
	contents1 := "contents of the first file"

	// start a backup, add a file
	bh, err := fbs.StartBackup(ctx, dir, name)
	require.NoError(t, err)
	wc, err := bh.AddFile(ctx, filename1, 0)
	require.NoError(t, err)
	_, err = wc.Write([]byte(contents1))
	require.NoError(t, err)
	require.NoError(t, wc.Close())

	// test we can't read back on read-write backup
	_, err = bh.ReadFile(ctx, filename1)
	require.Error(t, err, "was able to ReadFile to read-write backup")

	// and close
	require.NoError(t, bh.EndBackup(ctx))

	// re-read the file
	bhs, err := fbs.ListBackups(ctx, dir)
	require.NoError(t, err)
	require.Lenf(t, bhs, 1, "ListBackups after abort returned wrong return: %v", bhs)
	rc, err := bhs[0].ReadFile(ctx, filename1)
	require.NoError(t, err)
	buf := make([]byte, len(contents1)+10)
	n, err := rc.Read(buf)
	require.Truef(t, (err == nil || err == io.EOF) && n == len(contents1), "rc.Read returned wrong result: %v %#v", err, n)
	require.NoError(t, rc.Close())
}
