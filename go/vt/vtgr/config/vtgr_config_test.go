/*
Copyright 2021 The Vitess Authors.

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
