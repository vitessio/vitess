/*
Copyright 2023 The Vitess Authors.

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

package viperutil

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetConfigHandlingValue(t *testing.T) {
	v := viper.New()
	v.SetDefault("default", ExitOnConfigFileNotFound)
	v.SetConfigType("yaml")

	cfg := `
foo: 2
bar: "2" # not valid, defaults to "ignore" (0)
baz: error
duration: 10h
`
	err := v.ReadConfig(strings.NewReader(strings.NewReplacer("\t", "  ").Replace(cfg)))
	require.NoError(t, err)

	getHandlingValueFunc := getHandlingValue(v)
	assert.Equal(t, ErrorOnConfigFileNotFound, getHandlingValueFunc("foo"), "failed to get int value")
	assert.Equal(t, IgnoreConfigFileNotFound, getHandlingValueFunc("bar"), "failed to get int-like string value")
	assert.Equal(t, ErrorOnConfigFileNotFound, getHandlingValueFunc("baz"), "failed to get string value")
	assert.Equal(t, IgnoreConfigFileNotFound, getHandlingValueFunc("notset"), "failed to get value on unset key")
	assert.Equal(t, IgnoreConfigFileNotFound, getHandlingValueFunc("duration"), "failed to get value on duration key")
	assert.Equal(t, ExitOnConfigFileNotFound, getHandlingValueFunc("default"), "failed to get value on default key")
}

// TestLoadConfig tests that LoadConfig behaves in the way expected when the config file doesn't exist.
func TestLoadConfig(t *testing.T) {
	origConfigFile := configFile.Get()
	origConfigName := configName.Get()
	origConfigFileNotFoundHandling := configFileNotFoundHandling.Get()
	defer func() {
		configFile.Set(origConfigFile)
		configName.Set(origConfigName)
		configFileNotFoundHandling.Set(origConfigFileNotFoundHandling)
	}()

	t.Run("Ignore file not found error", func(t *testing.T) {
		configFile.Set("notfound.yaml")
		configFileNotFoundHandling.Set(IgnoreConfigFileNotFound)
		_, err := LoadConfig()
		require.NoError(t, err)
	})

	t.Run("Ignore file not found error from config name", func(t *testing.T) {
		configFile.Set("")
		configName.Set("notfound")
		configFileNotFoundHandling.Set(IgnoreConfigFileNotFound)
		_, err := LoadConfig()
		require.NoError(t, err)
	})

	t.Run("Warn file not found error", func(t *testing.T) {
		configFile.Set("notfound.yaml")
		configFileNotFoundHandling.Set(WarnOnConfigFileNotFound)
		_, err := LoadConfig()
		require.NoError(t, err)
	})

	t.Run("Ignore file not found error from config name", func(t *testing.T) {
		configFile.Set("")
		configName.Set("notfound")
		configFileNotFoundHandling.Set(WarnOnConfigFileNotFound)
		_, err := LoadConfig()
		require.NoError(t, err)
	})

	t.Run("Error file not found error", func(t *testing.T) {
		configFile.Set("notfound.yaml")
		configFileNotFoundHandling.Set(ErrorOnConfigFileNotFound)
		_, err := LoadConfig()
		require.Error(t, err)
	})

	t.Run("Ignore file not found error from config name", func(t *testing.T) {
		configFile.Set("")
		configName.Set("notfound")
		configFileNotFoundHandling.Set(ErrorOnConfigFileNotFound)
		_, err := LoadConfig()
		require.Error(t, err)
	})
}
