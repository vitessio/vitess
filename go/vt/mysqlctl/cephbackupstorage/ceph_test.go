/*
Copyright 2026 The Vitess Authors.

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

package cephbackupstorage

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStorage returns a plugin instance pointed at a fresh fake S3 server.
func newTestStorage(t *testing.T) (*CephBackupStorage, *fakeS3) {
	t.Helper()
	fake := newFakeS3()
	t.Cleanup(fake.close)
	bs := NewFakeCephBackupStorage(FakeConfig{
		AccessKey: "access",
		SecretKey: "secret",
		EndPoint:  fake.endpoint(),
		UseSSL:    false,
	})
	return bs, fake
}

// writeFile adds one file to a backup handle and waits for the upload. A
// negative size tells the plugin the size is unknown.
func writeFile(t *testing.T, bh *CephBackupHandle, name, contents string, size int64) {
	t.Helper()
	w, err := bh.AddFile(t.Context(), name, size)
	require.NoError(t, err)
	_, err = io.WriteString(w, contents)
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

func sized(s string) int64 { return int64(len(s)) }

func TestBackupRoundTrip(t *testing.T) {
	ctx := t.Context()
	bs, fake := newTestStorage(t)
	dir := "test_keyspace/-80"

	bh, err := bs.StartBackup(ctx, dir, "2026-09-13.100000")
	require.NoError(t, err)
	manifest := `{"files":1}`
	writeFile(t, bh.(*CephBackupHandle), "MANIFEST", manifest, sized(manifest))
	// Unknown size: the plugin passes -1 through to the client.
	writeFile(t, bh.(*CephBackupHandle), "data/ibdata1", "hello", -1)
	require.NoError(t, bh.EndBackup(ctx))

	// Bucket is the keyspace, lowercased, with "_" replaced by "-".
	assert.Equal(t, []string{
		"test_keyspace/-80/2026-09-13.100000/MANIFEST",
		"test_keyspace/-80/2026-09-13.100000/data/ibdata1",
	}, fake.objects("test-keyspace"))

	handles, err := bs.ListBackups(ctx, dir)
	require.NoError(t, err)
	require.Len(t, handles, 1)
	assert.Equal(t, "2026-09-13.100000", handles[0].Name())
	assert.Equal(t, dir, handles[0].Directory())

	rc, err := handles[0].ReadFile(ctx, "data/ibdata1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Close() })
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(got))
}

func TestListBackupsOrdersOldestFirst(t *testing.T) {
	ctx := t.Context()
	bs, _ := newTestStorage(t)
	dir := "ks/0"

	for _, name := range []string{"2026-09-13.120000", "2026-09-12.120000", "2026-09-13.090000"} {
		bh, err := bs.StartBackup(ctx, dir, name)
		require.NoError(t, err)
		writeFile(t, bh.(*CephBackupHandle), "MANIFEST", "{}", sized("{}"))
		require.NoError(t, bh.EndBackup(ctx))
	}

	handles, err := bs.ListBackups(ctx, dir)
	require.NoError(t, err)
	names := make([]string, 0, len(handles))
	for _, h := range handles {
		names = append(names, h.Name())
	}
	assert.Equal(t, []string{"2026-09-12.120000", "2026-09-13.090000", "2026-09-13.120000"}, names)
}

func TestListBackupsMissingBucketIsEmpty(t *testing.T) {
	bs, _ := newTestStorage(t)

	handles, err := bs.ListBackups(t.Context(), "never_created/0")
	require.NoError(t, err)
	assert.Empty(t, handles)
}

func TestRemoveBackupDeletesOnlyThatBackup(t *testing.T) {
	ctx := t.Context()
	bs, fake := newTestStorage(t)
	dir := "ks/0"

	for _, name := range []string{"keep", "remove"} {
		bh, err := bs.StartBackup(ctx, dir, name)
		require.NoError(t, err)
		writeFile(t, bh.(*CephBackupHandle), "MANIFEST", "{}", sized("{}"))
		writeFile(t, bh.(*CephBackupHandle), "data/f1", "x", sized("x"))
		require.NoError(t, bh.EndBackup(ctx))
	}

	require.NoError(t, bs.RemoveBackup(ctx, dir, "remove"))

	assert.Equal(t, []string{"ks/0/keep/MANIFEST", "ks/0/keep/data/f1"}, fake.objects("ks"))
	handles, err := bs.ListBackups(ctx, dir)
	require.NoError(t, err)
	require.Len(t, handles, 1)
	assert.Equal(t, "keep", handles[0].Name())
}

func TestAbortBackupRemovesUploadedFiles(t *testing.T) {
	ctx := t.Context()
	bs, fake := newTestStorage(t)

	bh, err := bs.StartBackup(ctx, "ks/0", "aborted")
	require.NoError(t, err)
	writeFile(t, bh.(*CephBackupHandle), "MANIFEST", "{}", sized("{}"))

	require.NoError(t, bh.AbortBackup(ctx))
	assert.Empty(t, fake.objects("ks"))
}

func TestReadOnlyAndReadWriteGuards(t *testing.T) {
	ctx := t.Context()
	bs, _ := newTestStorage(t)
	dir := "ks/0"

	rw, err := bs.StartBackup(ctx, dir, "b")
	require.NoError(t, err)
	_, err = rw.ReadFile(ctx, "MANIFEST")
	require.ErrorContains(t, err, "read-write backup")

	writeFile(t, rw.(*CephBackupHandle), "MANIFEST", "{}", sized("{}"))
	require.NoError(t, rw.EndBackup(ctx))

	handles, err := bs.ListBackups(ctx, dir)
	require.NoError(t, err)
	require.Len(t, handles, 1)
	ro := handles[0]

	_, err = ro.AddFile(ctx, "x", 1)
	require.ErrorContains(t, err, "read-only backup")
	require.ErrorContains(t, ro.EndBackup(ctx), "read-only backup")
	require.ErrorContains(t, ro.AbortBackup(ctx), "read-only backup")
}

// TestClientUsesSigV4AndPathStyle pins the two on-the-wire changes of the
// client swap: requests are signed with AWS Signature V4 (minio.NewV2 used V2)
// and the bucket is addressed path-style rather than as a subdomain of the
// endpoint.
func TestClientUsesSigV4AndPathStyle(t *testing.T) {
	bs, fake := newTestStorage(t)

	_, err := bs.StartBackup(t.Context(), "my_ks/0", "b")
	require.NoError(t, err)

	reqs := fake.recorded()
	require.NotEmpty(t, reqs)
	for _, r := range reqs {
		assert.True(t, strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256"),
			"%s %s: Authorization=%q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
		assert.True(t, strings.HasPrefix(r.URL.Path, "/my-ks"), "%s %s", r.Method, r.URL.Path)
		assert.Equal(t, fake.endpoint(), r.Host, "%s %s", r.Method, r.URL.Path)
	}
}

func TestEndpointScheme(t *testing.T) {
	tests := []struct {
		useSSL bool
		want   string
	}{
		{useSSL: false, want: "http://ceph.example:7480"},
		{useSSL: true, want: "https://ceph.example:7480"},
	}
	for _, tc := range tests {
		bs := NewFakeCephBackupStorage(FakeConfig{EndPoint: "ceph.example:7480", UseSSL: tc.useSSL})
		assert.Equal(t, tc.want, bs.endpointURL())
	}
}

func TestCloseReloadsConfigFile(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "ceph.json")
	writeConfig := func(endpoint string) {
		require.NoError(t, os.WriteFile(cfgPath,
			[]byte(`{"accessKey":"a","secretKey":"s","endPoint":"`+endpoint+`","useSSL":false}`), 0o600))
	}
	saved := configFilePath
	t.Cleanup(func() { configFilePath = saved })
	configFilePath = cfgPath

	writeConfig("first.example:7480")
	bs := &CephBackupStorage{}
	_, err := bs.client()
	require.NoError(t, err)
	assert.Equal(t, "http://first.example:7480", bs.endpointURL())

	require.NoError(t, bs.Close())
	writeConfig("second.example:7480")
	_, err = bs.client()
	require.NoError(t, err)
	assert.Equal(t, "http://second.example:7480", bs.endpointURL())

	// A test-supplied config survives Close and never touches the file.
	configFilePath = filepath.Join(t.TempDir(), "does-not-exist.json")
	fakeBS := NewFakeCephBackupStorage(FakeConfig{AccessKey: "a", SecretKey: "s", EndPoint: "fake.example:7480"})
	_, err = fakeBS.client()
	require.NoError(t, err)
	require.NoError(t, fakeBS.Close())
	_, err = fakeBS.client()
	require.NoError(t, err)
}
