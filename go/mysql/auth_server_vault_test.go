/*
copyright 2020 The Vitess Authors.

licensed under the apache license, version 2.0 (the "license");
you may not use this file except in compliance with the license.
you may obtain a copy of the license at

    http://www.apache.org/licenses/license-2.0

unless required by applicable law or agreed to in writing, software
distributed under the license is distributed on an "as is" basis,
without warranties or conditions of any kind, either express or implied.
see the license for the specific language governing permissions and
limitations under the license.
*/

package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestErrorConditions(t *testing.T) {
	// Bad token file path
	_, err := newAuthServerVault("localhost", 1*time.Second, "", "/path/to/secret/in/vault", 10*time.Second, "/tmp/this_file_does_not_exist", "", "", "")
	assert.Contains(t, err.Error(), "No Vault token in provided filename")

	// Bad secretID file path
	_, err = newAuthServerVault("localhost", 1*time.Second, "", "/path/to/secret/in/vault", 10*time.Second, "", "", "/tmp/this_file_does_not_exist", "")
	assert.Contains(t, err.Error(), "No Vault secret_id in provided filename")

	// Bad init ; but we should just retry
	a, err := newAuthServerVault("https://localhost:828", 1*time.Second, "", "/path/to/secret/in/vault", 10*time.Second, "", "", "", "")
	assert.NotEqual(t, a, nil)
	assert.Equal(t, err, nil)

	// Test reload, should surface error, since we don't have a Vault
	// instance on port 828
	err = a.reloadVault()
	assert.Contains(t, err.Error(), "Error in vtgate Vault auth server params")
	assert.Contains(t, err.Error(), "connection refused")

	a.close()
}
