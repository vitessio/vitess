package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadConfig(t *testing.T) {
	path, _ := os.Getwd()
	config, err := ReadVTGRConfig(filepath.Join(path, "vtgr_config.json"))
	assert.NoError(t, err)
	// Make sure VTGR config honors the default setting
	assert.Equal(t, false, config.DisableReadOnlyProtection)
	assert.Equal(t, 600, config.BootstrapWaitTimeSeconds)
	// Make sure the config is load correctly
	assert.Equal(t, 3, config.GroupSize)
	assert.Equal(t, 5, config.BackoffErrorWaitTimeSeconds)
}
