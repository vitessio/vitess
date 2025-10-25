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

package fileutil

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSafePathJoin(t *testing.T) {
	rootDir := t.TempDir()

	t.Run("success", func(t *testing.T) {
		rootDir := t.TempDir()
		path, err := SafePathJoin(rootDir, "good/path")
		require.NoError(t, err)
		require.True(t, filepath.IsAbs(path))
		require.Equal(t, filepath.Join(rootDir, "good/path"), path)
	})

	t.Run("dir-traversal", func(t *testing.T) {
		_, err := SafePathJoin(rootDir, "../../..")
		require.ErrorIs(t, err, ErrInvalidBackupDir)
	})
}
