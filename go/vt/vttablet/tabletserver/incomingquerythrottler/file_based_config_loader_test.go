package incomingquerythrottler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestNewFileBasedConfigLoader(t *testing.T) {
	loader := NewFileBasedConfigLoader()
	require.NotNil(t, loader)
	require.IsType(t, &FileBasedConfigLoader{}, loader)
}

func TestFileBasedConfigLoader_Load(t *testing.T) {
	tests := []struct {
		name                string
		stubOsReadFile      func(filename string) ([]byte, error)
		stubJsonUnmarshal   func(data []byte, v interface{}) error
		expectedConfig      Config
		expectedError       string
		expectedErrorNotNil bool
	}{
		{
			name: "successful config load with minimal config",
			stubOsReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return []byte(`{"enabled": true, "strategy": "TabletThrottler"}`), nil
			},
			stubJsonUnmarshal: func(data []byte, v interface{}) error {
				return json.Unmarshal(data, v)
			},
			expectedConfig: Config{
				Enabled:  true,
				Strategy: ThrottlingStrategyTabletThrottler,
			},
			expectedErrorNotNil: false,
		},
		{
			name: "successful config load with disabled throttler",
			stubOsReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return []byte(`{"enabled": false, "strategy": "Unknown"}`), nil
			},
			stubJsonUnmarshal: func(data []byte, v interface{}) error {
				return json.Unmarshal(data, v)
			},
			expectedConfig: Config{
				Enabled:  false,
				Strategy: ThrottlingStrategyUnknown,
			},
			expectedErrorNotNil: false,
		},
		{
			name: "file read error - file not found",
			stubOsReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return nil, errors.New("no such file or directory")
			},
			stubJsonUnmarshal: func(data []byte, v interface{}) error {
				// Should not be called
				t.Fatal("jsonUnmarshal should not be called when file read fails")
				return nil
			},
			expectedConfig:      Config{},
			expectedError:       "no such file or directory",
			expectedErrorNotNil: true,
		},
		{
			name: "file read error - permission denied",
			stubOsReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return nil, errors.New("permission denied")
			},
			stubJsonUnmarshal: func(data []byte, v interface{}) error {
				// Should not be called
				t.Fatal("jsonUnmarshal should not be called when file read fails")
				return nil
			},
			expectedConfig:      Config{},
			expectedError:       "permission denied",
			expectedErrorNotNil: true,
		},
		{
			name: "json unmarshal error - invalid json",
			stubOsReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return []byte(`{"enabled": true, "strategy": `), nil // incomplete JSON
			},
			stubJsonUnmarshal: func(data []byte, v interface{}) error {
				return errors.New("unexpected end of JSON input")
			},
			expectedConfig:      Config{},
			expectedError:       "unexpected end of JSON input",
			expectedErrorNotNil: true,
		},
		{
			name: "json unmarshal error - invalid field type",
			stubOsReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return []byte(`{"enabled": "invalid_boolean", "strategy": "TabletThrottler"}`), nil
			},
			stubJsonUnmarshal: func(data []byte, v interface{}) error {
				return errors.New("json: cannot unmarshal string into Go struct field Config.enabled of type bool")
			},
			expectedConfig:      Config{},
			expectedError:       "json: cannot unmarshal string into Go struct field Config.enabled of type bool",
			expectedErrorNotNil: true,
		},
		{
			name: "empty file - should unmarshal to zero value config",
			stubOsReadFile: func(filename string) ([]byte, error) {
				require.Equal(t, "/config/throttler-config.json", filename)
				return []byte(`{}`), nil
			},
			stubJsonUnmarshal: func(data []byte, v interface{}) error {
				return json.Unmarshal(data, v)
			},
			expectedConfig: Config{
				Enabled:  false, // zero value for bool
				Strategy: "",    // zero value for ThrottlingStrategy
			},
			expectedErrorNotNil: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create stubs for the global variables
			osReadFileStub := gostub.Stub(&_osReadFile, tt.stubOsReadFile)
			jsonUnmarshalStub := gostub.Stub(&_jsonUnmarshal, tt.stubJsonUnmarshal)

			// Ensure stubs are reset after the test
			defer osReadFileStub.Reset()
			defer jsonUnmarshalStub.Reset()

			// Create loader and test Load method
			loader := NewFileBasedConfigLoader()
			config, err := loader.Load(context.Background())

			// Verify error expectations
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
	// Test that the correct config path is being used
	var capturedPath string

	stubOsReadFile := func(filename string) ([]byte, error) {
		capturedPath = filename
		return []byte(`{"enabled": true, "strategy": "TabletThrottler"}`), nil
	}

	stubJsonUnmarshal := func(data []byte, v interface{}) error {
		return json.Unmarshal(data, v)
	}

	// Create stubs
	osReadFileStub := gostub.Stub(&_osReadFile, stubOsReadFile)
	jsonUnmarshalStub := gostub.Stub(&_jsonUnmarshal, stubJsonUnmarshal)

	defer osReadFileStub.Reset()
	defer jsonUnmarshalStub.Reset()

	// Test
	loader := NewFileBasedConfigLoader()
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
