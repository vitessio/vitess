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
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/log/logtest"
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

// TestGetConfigHandlingValueLogsInvalidValue checks that an invalid value is
// logged with its key, the error and the value used instead.
func TestGetConfigHandlingValueLogsInvalidValue(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	v := viper.New()
	v.Set("bad", "not-a-handling-value")

	assert.Equal(t, IgnoreConfigFileNotFound, getHandlingValue(v)("bad"))

	assert.Len(t, logs.Records(t), 1, "the jww line isn't also logged to go/vt/log")
	record := logs.Record(t, "failed to unmarshal config_key; defaulting to default_value")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "bad", record["config_key"])
	assert.Equal(t, "ignore", record["default_value"])
	assert.Contains(t, record["error"], "not-a-handling-value")
}

const exitTestEnv = "TEST_VIPERUTIL_EXIT"

// TestLoadConfigLogsNotFound checks the record logged when the config file
// isn't found and the handling is error or exit, and that a config looked up by
// name, which has no file name, is logged without a path. The exit runs in a
// child process.
func TestLoadConfigLogsNotFound(t *testing.T) {
	if os.Getenv(exitTestEnv) == t.Name() {
		log.SwapLogger(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
		configFile.Set("notfound.yaml")
		configFileNotFoundHandling.Set(ExitOnConfigFileNotFound)
		_, _ = LoadConfig()
		return
	}

	origConfigFile, origConfigName := configFile.Get(), configName.Get()
	origConfigFileNotFoundHandling := configFileNotFoundHandling.Get()
	t.Cleanup(func() {
		configFile.Set(origConfigFile)
		configName.Set(origConfigName)
		configFileNotFoundHandling.Set(origConfigFileNotFoundHandling)
	})

	logs := logtest.Capture(t, slog.LevelInfo)
	configFile.Set("notfound.yaml")
	configFileNotFoundHandling.Set(ErrorOnConfigFileNotFound)
	_, err := LoadConfig()
	require.Error(t, err)

	assert.Len(t, logs.Records(t), 1, "the jww line isn't also logged to go/vt/log")
	record := logs.Record(t, "Failed to read in config")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "notfound.yaml", record["path"])
	assert.Contains(t, record["error"], "no such file or directory")

	logs.Reset()
	configFile.Set("")
	configName.Set("notfound")
	_, err = LoadConfig()
	require.Error(t, err)

	record = logs.Record(t, "Failed to read in config")
	assert.Equal(t, "ERROR", record["level"])
	assert.Contains(t, record["error"], `"notfound" Not Found`)
	assert.NotContains(t, record, "path")

	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), exitTestEnv+"="+t.Name())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())

	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	record = nil
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &record), stderr.String())
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "Failed to read in config", record["msg"])
	assert.Equal(t, "notfound.yaml", record["path"])
}
