package vault

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
