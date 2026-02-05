/*
Copyright 2020 The Vitess Authors.

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

package cluster

import (
	"strings"

	"github.com/spf13/viper"
)

// FileConfig represents the structure of a set of cluster configs on disk. It
// contains both a default config, and cluster-specific overrides. Viper is used
// internally to load the config file, so any file format supported by viper is
// permitted.
//
// A valid YAML config looks like:
//
//	defaults:
//		discovery: k8s
//	clusters:
//		clusterID1:
//			name: clusterName1
//			discovery-k8s-some-flag: some-val
//		clusterID2:
//			name: clusterName2
//			discovery: consul
type FileConfig struct {
	Defaults Config
	Clusters map[string]Config
}

// String is part of the flag.Value interface.
func (fc *FileConfig) String() string {
	buf := strings.Builder{}

	// inlining this produces "String is not in the method set of ClustersFlag"
	cf := ClustersFlag(fc.Clusters)

	buf.WriteString("{defaults: ")
	buf.WriteString(fc.Defaults.String())
	buf.WriteString(", clusters: ")
	buf.WriteString(cf.String())
	buf.WriteString("}")

	return buf.String()
}

// Type is part of the pflag.Value interface.
func (fc *FileConfig) Type() string {
	return "cluster.FileConfig"
}

// Set is part of the flag.Value interface. It loads the file configuration
// found at the path passed to the flag. Any config file format supported by
// viper is supported by FileConfig.
func (fc *FileConfig) Set(value string) error {
	v := viper.New()
	v.SetConfigFile(value)
	if err := v.ReadInConfig(); err != nil {
		return err
	}

	return fc.unmarshalViper(v)
}

func (fc *FileConfig) unmarshalViper(v *viper.Viper) error {
	tmp := struct { // work around mapstructure's interface; see https://github.com/spf13/viper/issues/338#issuecomment-382376136
		Defaults map[string]string
		Clusters map[string]map[string]string
	}{
		Defaults: map[string]string{},
		Clusters: map[string]map[string]string{},
	}

	if err := v.Unmarshal(&tmp); err != nil {
		return err
	}

	if err := fc.Defaults.unmarshalMap(tmp.Defaults); err != nil {
		return err
	}

	if fc.Clusters == nil {
		fc.Clusters = map[string]Config{}
	}
	for id, clusterMap := range tmp.Clusters {
		c, ok := fc.Clusters[id]
		if !ok {
			c = Config{
				ID:                   id,
				DiscoveryFlagsByImpl: map[string]map[string]string{},
				VtSQLFlags:           map[string]string{},
				VtctldFlags:          map[string]string{},
			}
		}

		if err := c.unmarshalMap(clusterMap); err != nil {
			return err
		}

		fc.Clusters[id] = c
	}

	return nil
}

// Combine combines a FileConfig with a default Config and a ClustersFlag (each
// defined on the command-line) into a slice of final Configs that are suitable
// to use for cluster creation.
//
// Combination uses the following precedence:
// 1. Command-line cluster-specific overrides.
// 2. File-based cluster-specific overrides.
// 3. Command-line cluster defaults.
// 4. File-based cluster defaults.
func (fc *FileConfig) Combine(defaults Config, clusters map[string]Config) []Config {
	configs := make([]Config, 0, len(clusters))
	merged := map[string]bool{}

	combinedDefaults := fc.Defaults.Merge(defaults)

	for name, cfg := range fc.Clusters {
		merged[name] = true

		override, ok := clusters[name]
		if !ok {
			configs = append(configs, combinedDefaults.Merge(cfg))
			continue
		}

		combinedOverrides := cfg.Merge(override)
		configs = append(configs, combinedDefaults.Merge(combinedOverrides))
	}

	for name, cfg := range clusters {
		if _, ok := merged[name]; ok {
			continue
		}

		configs = append(configs, combinedDefaults.Merge(cfg))
	}

	return configs
}
