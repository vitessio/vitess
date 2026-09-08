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

package vreplication

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBTypeVersionArtifact(t *testing.T) {
	t.Run("mysql 8.0 has a build for both architectures", func(t *testing.T) {
		amd64, err := dbTypeVersionArtifact("mysql", "8.0", "linux", "amd64")
		require.NoError(t, err)
		assert.Equal(t, "mysql-8.0.28-linux-glibc2.17-x86_64-minimal.tar.xz", amd64.file)
		assert.Equal(t, "https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.28-linux-glibc2.17-x86_64-minimal.tar.xz", amd64.url)

		arm64, err := dbTypeVersionArtifact("mysql", "8.0", "linux", "arm64")
		require.NoError(t, err)
		assert.Equal(t, "mysql-8.0.46-linux-glibc2.28-aarch64.tar.xz", arm64.file)
		assert.Equal(t, "https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.46-linux-glibc2.28-aarch64.tar.xz", arm64.url)
	})

	t.Run("mysql 5.7 and mariadb 10.10 only exist for amd64", func(t *testing.T) {
		for _, tc := range []struct{ dbType, major string }{
			{"mysql", "5.7"},
			{"mariadb", "10.10"},
		} {
			_, err := dbTypeVersionArtifact(tc.dbType, tc.major, "linux", "amd64")
			require.NoError(t, err, "%s-%s on amd64", tc.dbType, tc.major)

			_, err = dbTypeVersionArtifact(tc.dbType, tc.major, "linux", "arm64")
			require.ErrorIs(t, err, errNoDBTypeVersionArtifact, "%s-%s on arm64", tc.dbType, tc.major)
			require.ErrorContains(t, err, tc.dbType+"-"+tc.major)
			require.ErrorContains(t, err, "linux/arm64")
		}
	})

	t.Run("unsupported OS is not a version we can skip over", func(t *testing.T) {
		_, err := dbTypeVersionArtifact("mysql", "8.0", "darwin", "arm64")
		require.ErrorIs(t, err, errNoDBTypeVersionArtifact)
	})

	t.Run("unknown version is an error, not a skip", func(t *testing.T) {
		_, err := dbTypeVersionArtifact("mysql", "9.9", "linux", "amd64")
		require.Error(t, err)
		require.NotErrorIs(t, err, errNoDBTypeVersionArtifact)
		require.ErrorContains(t, err, "unsupported major version")
	})
}
