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

package dbconfigs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestVaultCredentialsServerCacheExpiryRace exercises the cache expiry
// goroutine concurrently with GetUserAndPassword. It is meant to be run with
// the race detector (go test -race): access to cacheValid from the expiry
// goroutine must be synchronized with GetUserAndPassword.
func TestVaultCredentialsServerCacheExpiryRace(t *testing.T) {
	vcs, ok := AllCredentialsServers["vault"].(*VaultCredentialsServer)
	require.True(t, ok)

	origTTL := vaultCacheTTL
	vaultCacheTTL = time.Millisecond
	t.Cleanup(func() {
		vaultCacheTTL = origTTL
		vcs.mu.Lock()
		defer vcs.mu.Unlock()
		if vcs.vaultCacheExpireTicker != nil {
			vcs.vaultCacheExpireTicker.Stop()
			vcs.vaultCacheExpireTicker = nil
		}
		vcs.dbCredsCache = nil
		vcs.cacheValid.Store(false)
	})

	vcs.mu.Lock()
	vcs.dbCredsCache = map[string][]string{"user": {"pass"}}
	vcs.cacheValid.Store(true)
	vcs.mu.Unlock()

	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		// Once the cache expires there is no Vault configured, so an error is
		// expected. We only care about the concurrent access to cacheValid.
		_, _, _ = vcs.GetUserAndPassword("user")
		time.Sleep(100 * time.Microsecond)
	}
}
