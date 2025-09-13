/*
Copyright 2025 The Vitess Authors.

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

package querythrottler

import (
	"context"
	"encoding/json"
	"os"
)

const defaultConfigPath = "/config/throttler-config.json"

var _ ConfigLoader = (*FileBasedConfigLoader)(nil)

// FileBasedConfigLoader implements ConfigLoader by reading configuration from a JSON file.
type FileBasedConfigLoader struct {
	configPath string
	readFile   func(string) ([]byte, error)
	unmarshal  func([]byte, interface{}) error
}

// NewFileBasedConfigLoader creates a new instance of FileBasedConfigLoader.
// It uses the standard config path "/config/throttler-config.json" and standard os.ReadFile and json.Unmarshal functions.
func NewFileBasedConfigLoader() *FileBasedConfigLoader {
	return &FileBasedConfigLoader{
		configPath: defaultConfigPath,
		readFile:   os.ReadFile,
		unmarshal:  json.Unmarshal,
	}
}

// NewFileBasedConfigLoaderWithDeps creates a new instance with custom dependencies for testing.
// This allows injection of mock functions without global state modification.
func NewFileBasedConfigLoaderWithDeps(configPath string, readFile func(string) ([]byte, error), unmarshal func([]byte, interface{}) error) *FileBasedConfigLoader {
	return &FileBasedConfigLoader{
		configPath: configPath,
		readFile:   readFile,
		unmarshal:  unmarshal,
	}
}

// Load reads the configuration from the configured file path.
func (f *FileBasedConfigLoader) Load(ctx context.Context) (Config, error) {
	data, err := f.readFile(f.configPath)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if unMarshalErr := f.unmarshal(data, &cfg); unMarshalErr != nil {
		return Config{}, unMarshalErr
	}

	return cfg, nil
}
