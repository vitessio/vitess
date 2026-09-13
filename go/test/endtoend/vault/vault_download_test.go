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

package vault

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVaultRelease(t *testing.T) {
	amd64, err := vaultRelease("linux", "amd64")
	require.NoError(t, err)
	assert.Equal(t, "https://releases.hashicorp.com/vault/1.6.1/vault_1.6.1_linux_amd64.zip", amd64.url)
	assert.Equal(t, "75cd2b8c5527577c0da1105e11fba3c31f4112514a910c4f7ec527c9a8bf42d1", amd64.sha256)

	arm64, err := vaultRelease("linux", "arm64")
	require.NoError(t, err)
	assert.Equal(t, "https://releases.hashicorp.com/vault/1.6.1/vault_1.6.1_linux_arm64.zip", arm64.url)
	assert.Equal(t, "09e9fc0a69350d49a5db90c51b19a2a63b1da060eeeed700109fb43e544ba947", arm64.sha256)

	_, err = vaultRelease("linux", "riscv64")
	require.ErrorContains(t, err, "linux/riscv64")
}
