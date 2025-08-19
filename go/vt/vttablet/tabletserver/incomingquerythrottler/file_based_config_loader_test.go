package incomingquerythrottler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/incomingquerythrottler/registry"

	"github.com/stretchr/testify/require"
)

func TestNewFileBasedConfigLoader(t *testing.T) {
	loader := NewFileBasedConfigLoader()
	require.NotNil(t, loader)
	require.IsType(t, &FileBasedConfigLoader{}, loader)
	require.Equal(t, defaultConfigPath, loader.configPath)
}

func TestFileBasedConfigLoader_Load(t *testing.T) {
	tests := []struct {
		name                string
		configPath          string
		mockReadFile        func(filename string) ([]byte, error)
		mockJsonUnmarshal   func(data []byte, v interface{}) error
		expectedConfig      Config
		expectedError       string
		expectedErrorNotNil bool
	}{
		{
			name:       "successful config load with minimal config",
			configPath: "/config/throttler-config.json",
			mockReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return []byte(`{"enabled": true, "strategy": "TabletThrottler"}`), nil
			},
			mockJsonUnmarshal: func(data []byte, v interface{}) error {
				return json.Unmarshal(data, v)
			},
			expectedConfig: Config{
				Enabled:  true,
				Strategy: registry.ThrottlingStrategyTabletThrottler,
			},
			expectedErrorNotNil: false,
		},
		{
			name:       "successful config load with disabled throttler",
			configPath: "/config/throttler-config.json",
			mockReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return []byte(`{"enabled": false, "strategy": "TabletThrottler"}`), nil
			},
			mockJsonUnmarshal: func(data []byte, v interface{}) error {
				return json.Unmarshal(data, v)
			},
			expectedConfig: Config{
				Enabled:  false,
				Strategy: registry.ThrottlingStrategyTabletThrottler,
			},
			expectedErrorNotNil: false,
		},
		{
			name:       "file read error - file not found",
			configPath: "/nonexistent/config.json",
			mockReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/nonexistent/config.json", filename)
				return nil, errors.New("no such file or directory")
			},
			mockJsonUnmarshal: func(data []byte, v interface{}) error {
				return json.Unmarshal(data, v)
			},
			expectedConfig:      Config{},
			expectedError:       "no such file or directory",
			expectedErrorNotNil: true,
		},
		{
			name:       "file read error - permission denied",
			configPath: "/config/throttler-config.json",
			mockReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return nil, errors.New("permission denied")
			},
			mockJsonUnmarshal: func(data []byte, v interface{}) error {
				return json.Unmarshal(data, v)
			},
			expectedConfig:      Config{},
			expectedError:       "permission denied",
			expectedErrorNotNil: true,
		},
		{
			name:       "json unmarshal error - invalid json",
			configPath: "/config/throttler-config.json",
			mockReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return []byte(`{"enabled": true`), nil
			},
			mockJsonUnmarshal: func(data []byte, v interface{}) error {
				return json.Unmarshal(data, v)
			},
			expectedConfig:      Config{},
			expectedError:       "unexpected end of JSON input",
			expectedErrorNotNil: true,
		},
		{
			name:       "json unmarshal error - invalid field type",
			configPath: "/config/throttler-config.json",
			mockReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return []byte(`{"enabled": "not_a_boolean", "strategy": "TabletThrottler"}`), nil
			},
			mockJsonUnmarshal: func(data []byte, v interface{}) error {
				return json.Unmarshal(data, v)
			},
			expectedConfig:      Config{},
			expectedError:       "cannot unmarshal string into Go struct field Config.enabled of type bool",
			expectedErrorNotNil: true,
		},
		{
			name:       "empty file - should unmarshal to zero value config",
			configPath: "/config/throttler-config.json",
			mockReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return []byte(`{}`), nil
			},
			mockJsonUnmarshal: func(data []byte, v interface{}) error {
				return json.Unmarshal(data, v)
			},
			expectedConfig: Config{
				Enabled:  false,
				Strategy: "",
			},
			expectedErrorNotNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create loader with injected dependencies
			loader := NewFileBasedConfigLoaderWithDeps(tt.configPath, tt.mockReadFile, tt.mockJsonUnmarshal)

			// Test
			config, err := loader.Load(context.Background())

			// Assert
			if tt.expectedErrorNotNil {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				require.Equal(t, tt.expectedConfig, config)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedConfig, config)
			}
		})
	}
}

func TestFileBasedConfigLoader_Load_ConfigPath(t *testing.T) {
	// Test that the production loader uses the default config path
	var capturedPath string

	mockReadFile := func(filename string) ([]byte, error) {
		capturedPath = filename
		return []byte(`{"enabled": true, "strategy": "TabletThrottler"}`), nil
	}

	mockJsonUnmarshal := func(data []byte, v interface{}) error {
		return json.Unmarshal(data, v)
	}

	// Test with production constructor (should use default path)
	loader := NewFileBasedConfigLoaderWithDeps(defaultConfigPath, mockReadFile, mockJsonUnmarshal)
	_, err := loader.Load(context.Background())

	require.NoError(t, err)
	require.Equal(t, "/config/throttler-config.json", capturedPath)
}

func TestFileBasedConfigLoader_ImplementsConfigLoader(t *testing.T) {
	// Verify that FileBasedConfigLoader implements ConfigLoader interface
	var _ ConfigLoader = (*FileBasedConfigLoader)(nil)

	// This should compile without issues, proving interface compliance
	loader := NewFileBasedConfigLoader()
	require.NotNil(t, loader)
}

func TestNewFileBasedConfigLoaderWithDeps(t *testing.T) {
	configPath := "/test/config.json"
	mockReadFile := func(string) ([]byte, error) { return nil, nil }
	mockUnmarshal := func([]byte, interface{}) error { return nil }

	loader := NewFileBasedConfigLoaderWithDeps(configPath, mockReadFile, mockUnmarshal)

	require.NotNil(t, loader)
	require.Equal(t, configPath, loader.configPath)
	// Note: We can't directly test function equality, but the constructor should set them
}

func TestFileBasedConfigLoader_UsesDefaultPath(t *testing.T) {
	// Test that the production constructor uses the default path
	loader := NewFileBasedConfigLoader()
	require.Equal(t, "/config/throttler-config.json", loader.configPath)
}
