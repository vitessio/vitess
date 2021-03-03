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
	"io/ioutil"
	"strings"

	"gopkg.in/yaml.v2"
)

// FileConfig represents the structure of a set of cluster configs on disk. It
// contains both a default config, and cluster-specific overrides. Currently
// only YAML config files are supported.
//
// A valid config looks like:
//		defaults:
//			discovery: k8s
//		clusters:
//			clusterID1:
//				name: clusterName1
//				discovery-k8s-some-flag: some-val
//			clusterID2:
//				name: clusterName2
//				discovery: consul
type FileConfig struct {
	Defaults Config
	Clusters map[string]Config
}

// UnmarshalYAML is part of the yaml.Unmarshaler interface.
func (fc *FileConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	tmp := struct {
		Defaults Config
		Clusters map[string]Config
	}{
		Defaults: fc.Defaults,
		Clusters: fc.Clusters,
	}

	if err := unmarshal(&tmp); err != nil {
		return err
	}

	fc.Defaults = tmp.Defaults
	fc.Clusters = make(map[string]Config, len(tmp.Clusters))

	for id, cfg := range tmp.Clusters {
		fc.Clusters[id] = cfg.Merge(Config{ID: id})
	}

	return nil
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
// found at the path passed to the flag.
func (fc *FileConfig) Set(value string) error {
	data, err := ioutil.ReadFile(value)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(data, fc)
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
