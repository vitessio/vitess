package incomingquerythrottler

import (
	"context"
	"encoding/json"
	"os"
)

var (
	_              ConfigLoader = (*FileBasedConfigLoader)(nil)
	_osReadFile                 = os.ReadFile
	_jsonUnmarshal              = json.Unmarshal
	_configPath                 = "/config/throttler-config.json"
)

type FileBasedConfigLoader struct{}

// NewFileBasedConfigLoader creates a new instance of FileBasedConfigLoader with the given file path.
func NewFileBasedConfigLoader() *FileBasedConfigLoader {
	return &FileBasedConfigLoader{}
}

// Load reads the configuration from a file at the specific config path.
func (f *FileBasedConfigLoader) Load(ctx context.Context) (Config, error) {
	data, err := _osReadFile(_configPath)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if unMarshalErr := _jsonUnmarshal(data, &cfg); unMarshalErr != nil {
		return Config{}, unMarshalErr
	}

	return cfg, nil
}
