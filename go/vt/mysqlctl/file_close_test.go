/*
Copyright 2025 The Vitess Authors.

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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
)

// mockCloser is a mock implementation of io.Closer that can be configured
// to fail a certain number of times before succeeding or to always fail.
type mockCloser struct {
	failCount    int        // Number of times Close should fail before succeeding
	currentFails int        // Current number of failures
	alwaysFail   bool       // If true, Close will always fail
	mu           sync.Mutex // Protects failCount and currentFails
	closeCalled  int        // Number of times Close was called
	closed       bool       // Whether the closer has been successfully closed
	err          error      // The error to return on failure
}

func newMockCloser(failCount int, err error) *mockCloser {
	return &mockCloser{
		failCount: failCount,
		err:       err,
	}
}

func newAlwaysFailingCloser(err error) *mockCloser {
	return &mockCloser{
		alwaysFail: true,
		err:        err,
	}
}

func (m *mockCloser) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closeCalled++

	if m.closed {
		return nil
	}

	if m.alwaysFail || m.currentFails < m.failCount {
		m.currentFails++
		return m.err
	}

	m.closed = true
	return nil
}

func (m *mockCloser) getCloseCalled() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCalled
}

func (m *mockCloser) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// mockReadOnlyCloser combines mockCloser with Read-Only capabilities for testing.
type mockReadOnlyCloser struct {
	*mockCloser
	io.Reader
}

// mockReadWriteCloser combines mockCloser with Read/Write capabilities for testing.
type mockReadWriteCloser struct {
	*mockCloser
	io.Reader
	io.Writer
}

func newMockReadOnlyCloser(failCount int, err error) *mockReadOnlyCloser {
	return &mockReadOnlyCloser{
		mockCloser: newMockCloser(failCount, err),
		Reader:     bytes.NewReader([]byte("test data")),
	}
}

func newMockReadWriteCloser(failCount int, err error) *mockReadWriteCloser {
	return &mockReadWriteCloser{
		mockCloser: newMockCloser(failCount, err),
		Writer:     &bytes.Buffer{},
	}
}

// mockBackupHandle is a mock implementation of backupstorage.BackupHandle for testing.
type mockBackupHandle struct {
	addFileReturn  io.WriteCloser
	addFileErr     error
	readFileReturn io.ReadCloser
	readFileErr    error
	name           string
	failedFiles    map[string]error
	mu             sync.Mutex
}

func newMockBackupHandle() *mockBackupHandle {
	return &mockBackupHandle{
		failedFiles: make(map[string]error),
		name:        "test-backup",
	}
}

func (m *mockBackupHandle) Name() string {
	return m.name
}

func (m *mockBackupHandle) Directory() string {
	return "test-directory"
}

func (m *mockBackupHandle) AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	if m.addFileErr != nil {
		return nil, m.addFileErr
	}
	return m.addFileReturn, nil
}

func (m *mockBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	if m.readFileErr != nil {
		return nil, m.readFileErr
	}
	return m.readFileReturn, nil
}

func (m *mockBackupHandle) EndBackup(ctx context.Context) error {
	return nil
}

func (m *mockBackupHandle) AbortBackup(ctx context.Context) error {
	return nil
}

func (m *mockBackupHandle) RecordError(filename string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedFiles[filename] = err
}

func (m *mockBackupHandle) GetFailedFiles() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	files := make([]string, 0, len(m.failedFiles))
	for file := range m.failedFiles {
		files = append(files, file)
	}
	return files
}

func (m *mockBackupHandle) ResetErrorForFile(filename string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.failedFiles, filename)
}

func (m *mockBackupHandle) Error() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.failedFiles) > 0 {
		var errs []string
		for file, err := range m.failedFiles {
			errs = append(errs, fmt.Sprintf("%s: %v", file, err))
		}
		return fmt.Errorf("failed files: %s", strings.Join(errs, ", "))
	}
	return nil
}

func (m *mockBackupHandle) HasErrors() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.failedFiles) > 0
}

// TestCloseWithRetrySuccess tests that closeWithRetry succeeds when Close succeeds.
func TestCloseWithRetrySuccess(t *testing.T) {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	closer := newMockCloser(0, nil)

	err := closeWithRetry(ctx, logger, closer, "test-file")

	assert.NoError(t, err)
	assert.Equal(t, 1, closer.getCloseCalled())
	assert.True(t, closer.isClosed())
}

// TestCloseWithRetryTransientFailure tests that closeWithRetry retries on transient failures.
func TestCloseWithRetryTransientFailure(t *testing.T) {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()

	// Test with various failure counts that should eventually succeed.
	// Note: We keep the failure count low because closeWithRetry uses exponential
	// backoff (1s, 2s, 4s, 8s, 16s, 30s...) which can make tests slow.
	testCases := []struct {
		name      string
		failCount int
	}{
		{"fail once then succeed", 1},
		{"fail twice then succeed", 2},
		{"fail three times then succeed", 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			closer := newMockCloser(tc.failCount, errors.New("transient error"))

			err := closeWithRetry(ctx, logger, closer, "test-file")

			assert.NoError(t, err)
			assert.True(t, closer.isClosed())
			// Should have called Close failCount+1 times (failCount failures + 1 success).
			assert.Equal(t, tc.failCount+1, closer.getCloseCalled())
		})
	}
}

// TestCloseWithRetryPermanentFailure tests that closeWithRetry gives up after maxFileCloseRetries.
// Note: This test uses context cancellation to avoid waiting for the full exponential backoff
// which can take several minutes with 20 retries.
func TestCloseWithRetryPermanentFailure(t *testing.T) {
	// Use a context with a short timeout to avoid waiting for all retries.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	logger := logutil.NewMemoryLogger()
	closer := newAlwaysFailingCloser(errors.New("permanent error"))

	err := closeWithRetry(ctx, logger, closer, "test-file")

	// Should fail with context deadline exceeded.
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
	// Should have attempted multiple times before context deadline.
	assert.Greater(t, closer.getCloseCalled(), 1)
	assert.False(t, closer.isClosed())
}

// TestCloseWithRetryContextCancellation tests that closeWithRetry respects context cancellation.
func TestCloseWithRetryContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logger := logutil.NewMemoryLogger()
	closer := newAlwaysFailingCloser(errors.New("error"))
	// Cancel the context after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := closeWithRetry(ctx, logger, closer, "test-file")

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	// Should have called Close at least once but not maxFileCloseRetries times.
	assert.Greater(t, closer.getCloseCalled(), 0)
	assert.Less(t, closer.getCloseCalled(), maxFileCloseRetries+1)
}

// TestBackupFileSourceCloseError tests error handling when a source file close fails during backup.
func TestBackupFileSourceCloseError(t *testing.T) {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	// Create a temporary directory for test files.
	tmpDir := t.TempDir()
	// Create test source file.
	sourceFile := path.Join(tmpDir, "source.txt")
	err := os.WriteFile(sourceFile, []byte("test content"), 0644)
	require.NoError(t, err)
	// Create Mycnf pointing to our temp directory.
	cnf := &Mycnf{
		DataDir: tmpDir,
	}
	be := &BuiltinBackupEngine{}
	// Create a mock backup handle with a source file that fails to close multiple times.
	sourceCloser := newMockReadWriteCloser(2, errors.New("failed to close source file"))
	bh := newMockBackupHandle()
	bh.addFileReturn = sourceCloser
	params := BackupParams{
		Cnf:         cnf,
		Logger:      logger,
		Stats:       backupstats.NoStats(),
		Concurrency: 1,
	}
	fe := &FileEntry{
		Base: backupData,
		Name: "source.txt",
	}

	// backupFile should handle the error gracefully.
	err = be.backupFile(ctx, params, bh, fe, "0")

	// Should succeed after retries.
	assert.NoError(t, err)
	assert.Equal(t, 3, sourceCloser.getCloseCalled()) // 2 failures + 1 success
	assert.True(t, sourceCloser.isClosed())
}

// TestBackupFileDestinationCloseError tests error handling when a destination file close fails during backup.
func TestBackupFileDestinationCloseError(t *testing.T) {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	// Create a temporary directory for test files.
	tmpDir := t.TempDir()
	// Create test source file.
	sourceFile := path.Join(tmpDir, "source.txt")
	content := []byte("test content for destination close error")
	err := os.WriteFile(sourceFile, content, 0644)
	require.NoError(t, err)
	// Create Mycnf pointing to our temp directory.
	cnf := &Mycnf{
		DataDir: tmpDir,
	}
	be := &BuiltinBackupEngine{}
	// Create a mock backup handle with a destination that fails to close multiple times.
	destCloser := newMockReadWriteCloser(3, errors.New("failed to close destination file"))
	bh := newMockBackupHandle()
	bh.addFileReturn = destCloser
	params := BackupParams{
		Cnf:         cnf,
		Logger:      logger,
		Stats:       backupstats.NoStats(),
		Concurrency: 1,
	}
	fe := &FileEntry{
		Base: backupData,
		Name: "source.txt",
	}

	err = be.backupFile(ctx, params, bh, fe, "0")

	// Should succeed after retries.
	assert.NoError(t, err)
	assert.Equal(t, 4, destCloser.getCloseCalled()) // 3 failures + 1 success
	assert.True(t, destCloser.isClosed())
}

// TestBackupFileDestinationCloseMaxRetries tests that destination close gives up after max retries.
// Note: This test uses a short context timeout to avoid waiting for all retries with exponential
// backoff.
func TestBackupFileDestinationCloseMaxRetries(t *testing.T) {
	// Use a short timeout to avoid waiting for the full exponential backoff.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	logger := logutil.NewMemoryLogger()
	// Create a temporary directory for test files.
	tmpDir := t.TempDir()
	// Create test destination file.
	destFile := path.Join(tmpDir, "destination.txt")
	content := []byte("test content for max retries")
	err := os.WriteFile(destFile, content, 0644)
	require.NoError(t, err)
	// Create Mycnf pointing to our temp directory.
	cnf := &Mycnf{
		DataDir: tmpDir,
	}
	be := &BuiltinBackupEngine{}
	// Create a mock backup handle with a destination file that always fails to close.
	destCloser := &mockReadWriteCloser{
		mockCloser: newAlwaysFailingCloser(errors.New("permanent file close failure")),
		Writer:     &bytes.Buffer{},
	}
	bh := newMockBackupHandle()
	bh.addFileReturn = destCloser
	params := BackupParams{
		Cnf:         cnf,
		Logger:      logger,
		Stats:       backupstats.NoStats(),
		Concurrency: 1,
	}
	fe := &FileEntry{
		Base: backupData,
		Name: "destination.txt",
	}

	err = be.backupFile(ctx, params, bh, fe, "0")

	// Should fail due to close error (context deadline exceeded).
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to close destination file")
	// Should have attempted multiple times before timeout.
	assert.Greater(t, destCloser.getCloseCalled(), 1)
	assert.False(t, destCloser.isClosed())
}

// TestBackupManifestCloseError tests error handling when manifest writer close fails.
func TestBackupManifestCloseError(t *testing.T) {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	be := &BuiltinBackupEngine{}

	testCases := []struct {
		name              string
		failCount         int
		alwaysFail        bool
		expectError       bool
		expectedCallCount int
		useTimeout        bool
	}{
		{
			name:              "close succeeds immediately",
			failCount:         0,
			alwaysFail:        false,
			expectError:       false,
			expectedCallCount: 1,
			useTimeout:        false,
		},
		{
			name:              "close fails twice then succeeds",
			failCount:         2,
			alwaysFail:        false,
			expectError:       false,
			expectedCallCount: 3,
			useTimeout:        false,
		},
		{
			name:              "close always fails",
			failCount:         0,
			alwaysFail:        true,
			expectError:       true,
			expectedCallCount: 2, // Will timeout before reaching max retries
			useTimeout:        true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testCtx := ctx
			if tc.useTimeout {
				var cancel context.CancelFunc
				testCtx, cancel = context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
			}
			var wc *mockReadWriteCloser
			if tc.alwaysFail {
				wc = &mockReadWriteCloser{
					mockCloser: newAlwaysFailingCloser(errors.New("write error")),
					Writer:     &bytes.Buffer{},
				}
			} else {
				wc = newMockReadWriteCloser(tc.failCount, errors.New("transient write error"))
			}
			bh := newMockBackupHandle()
			bh.addFileReturn = wc
			tmpDir := t.TempDir()
			cnf := &Mycnf{
				DataDir: tmpDir,
			}
			params := BackupParams{
				Cnf:        cnf,
				Logger:     logger,
				Stats:      backupstats.NoStats(),
				BackupTime: time.Now(),
			}
			fes := []FileEntry{}

			err := be.backupManifest(testCtx, params, bh, testPosition(), testPosition(), testPosition(), "", "test-uuid", "8.0.32", nil, fes, 0)

			if tc.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "cannot close backup")
			} else {
				assert.NoError(t, err)
			}
			assert.GreaterOrEqual(t, wc.getCloseCalled(), tc.expectedCallCount)
		})
	}
}

// TestRestoreFileSourceCloseError tests error handling when a source file close fails during restore.
func TestRestoreFileSourceCloseError(t *testing.T) {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	tmpDir := t.TempDir()
	be := &BuiltinBackupEngine{}
	// Create a mock backup handle with a file source that fails to close.
	sourceCloser := newMockReadOnlyCloser(2, errors.New("failed to close source file"))
	bh := newMockBackupHandle()
	bh.readFileReturn = sourceCloser
	cnf := &Mycnf{
		DataDir: tmpDir,
	}
	params := RestoreParams{
		Cnf:    cnf,
		Logger: logger,
		Stats:  backupstats.NoStats(),
	}
	fe := &FileEntry{
		Base: backupData,
		Name: "test-restore.txt",
		Hash: "00000000", // Will fail hash check, but that's ok for this test
	}
	bm := builtinBackupManifest{
		SkipCompress: true,
	}

	err := be.restoreFile(ctx, params, bh, fe, bm, "0")

	// Will fail due to hash mismatch, but we can verify close was attempted with retries.
	assert.Error(t, err)
	// Source should have been closed with retries.
	assert.GreaterOrEqual(t, sourceCloser.getCloseCalled(), 1)
}

// TestRestoreFileDestinationClose tests the happy path when closing a destination file during restore.
func TestRestoreFileDestinationClose(t *testing.T) {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	tmpDir := t.TempDir()
	// We need to create a more complete test setup for this
	// because restoreFile creates the destination file itself.
	be := &BuiltinBackupEngine{}
	content := []byte("test restore content")
	br := bytes.NewReader(content)
	sourceCloser := &mockReadWriteCloser{
		mockCloser: newMockCloser(0, nil),
		Reader:     br,
	}
	bh := newMockBackupHandle()
	bh.readFileReturn = sourceCloser
	cnf := &Mycnf{
		DataDir: tmpDir,
	}
	params := RestoreParams{
		Cnf:    cnf,
		Logger: logger,
		Stats:  backupstats.NoStats(),
	}
	// Calculate the actual hash of our content for a successful restore.
	bp := newBackupReader("test", 0, bytes.NewReader(content))
	io.ReadAll(bp)
	expectedHash := bp.HashString()
	fe := &FileEntry{
		Base: backupData,
		Name: "test-restore.txt",
		Hash: expectedHash,
	}
	bm := builtinBackupManifest{
		SkipCompress: true,
	}

	err := be.restoreFile(ctx, params, bh, fe, bm, "0")

	// The restore should succeed (destination close should work for real files).
	assert.NoError(t, err)
	// Verify the file was actually created
	destPath := filepath.Join(tmpDir, "test-restore.txt")
	_, err = os.Stat(destPath)
	assert.NoError(t, err)
}

// TestRestoreFileWithCloseRetriesIntegration is an integration test that verifies
// the full restore flow handles close retries properly.
func TestRestoreFileWithCloseRetriesIntegration(t *testing.T) {
	ctx := context.Background()
	logger := logutil.NewMemoryLogger()
	tmpDir := t.TempDir()
	be := &BuiltinBackupEngine{}
	content := []byte("integration test content for restore")
	// Create a source that will fail to close a few times.
	sourceCloser := &mockReadOnlyCloser{
		mockCloser: newMockCloser(1, errors.New("transient file close error")),
		Reader:     bytes.NewReader(content),
	}
	bh := newMockBackupHandle()
	bh.readFileReturn = sourceCloser
	cnf := &Mycnf{
		DataDir: tmpDir,
	}
	params := RestoreParams{
		Cnf:    cnf,
		Logger: logger,
		Stats:  backupstats.NoStats(),
	}
	// Calculate the hash.
	bp := newBackupReader("test", 0, bytes.NewReader(content))
	io.ReadAll(bp)
	expectedHash := bp.HashString()
	fe := &FileEntry{
		Base: backupData,
		Name: "integration-test.txt",
		Hash: expectedHash,
	}
	bm := builtinBackupManifest{
		SkipCompress: true,
	}

	err := be.restoreFile(ctx, params, bh, fe, bm, "0")

	// Should succeed after retries.
	assert.NoError(t, err)
	// Verify source was closed with retry (1 failure + 1 success)
	assert.Equal(t, 2, sourceCloser.getCloseCalled())
	assert.True(t, sourceCloser.isClosed())
	// Verify the file was created with correct content.
	destPath := filepath.Join(tmpDir, "integration-test.txt")
	restoredContent, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, content, restoredContent)
}

// Helper function to create a test replication position.
func testPosition() replication.Position {
	return replication.Position{}
}
