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
