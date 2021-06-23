package config

import (
	"encoding/json"
	"os"
)

// VTGRConfig is the config for VTGR
type VTGRConfig struct {
	DisableReadOnlyProtection   bool
	GroupSize                   int
	MinNumReplica               int
	BackoffErrorWaitTimeSeconds int
	BootstrapWaitTimeSeconds    int
}

var vtgrCfg = newVTGRConfig()

func newVTGRConfig() *VTGRConfig {
	config := &VTGRConfig{
		DisableReadOnlyProtection:   false,
		GroupSize:                   5,
		MinNumReplica:               3,
		BackoffErrorWaitTimeSeconds: 10,
		BootstrapWaitTimeSeconds:    10 * 60,
	}
	return config
}

// ReadVTGRConfig reads config for VTGR
func ReadVTGRConfig(file string) (*VTGRConfig, error) {
	vtgrFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(vtgrFile)
	err = decoder.Decode(vtgrCfg)
	if err != nil {
		return nil, err
	}
	return vtgrCfg, nil
}
