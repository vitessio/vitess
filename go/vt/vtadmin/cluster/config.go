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

import "fmt"

// Config represents the options to configure a vtadmin cluster.
type Config struct {
	ID                   string
	Name                 string
	DiscoveryImpl        string
	DiscoveryFlagsByImpl FlagsByImpl
	VtSQLFlags           map[string]string
	VtctldFlags          map[string]string
}

// Cluster returns a new cluster instance from the given config.
func (cfg Config) Cluster() (*Cluster, error) {
	return New(cfg)
}

// String is part of the flag.Value interface.
func (cfg *Config) String() string { return fmt.Sprintf("%T:%+v", cfg, *cfg) }

// Type is part of the pflag.Value interface.
func (cfg *Config) Type() string { return "cluster.Config" }

// Set is part of the flag.Value interface. Each flag is parsed according to the
// following DSN:
//
// 		id= // ID or shortname of the cluster.
//		name= // Name of the cluster.
// 		discovery= // Name of the discovery implementation
// 		discovery-.*= // Per-discovery-implementation flags. These are passed to
//		              // a given discovery implementation's constructor.
//		vtsql-.*= // VtSQL-specific flags. Further parsing of these is delegated
// 		          // to the vtsql package.
func (cfg *Config) Set(value string) error {
	if cfg.DiscoveryFlagsByImpl == nil {
		cfg.DiscoveryFlagsByImpl = map[string]map[string]string{}
	}

	return parseFlag(cfg, value)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (cfg *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	attributes := map[string]string{}

	if err := unmarshal(attributes); err != nil {
		return err
	}

	for k, v := range attributes {
		if err := parseOne(cfg, k, v); err != nil {
			return err
		}
	}

	return nil
}

// Merge returns the result of merging the calling config into the passed
// config. Neither the caller or the argument are modified in any way.
func (cfg Config) Merge(override Config) Config {
	merged := Config{
		ID:                   cfg.ID,
		Name:                 cfg.Name,
		DiscoveryImpl:        cfg.DiscoveryImpl,
		DiscoveryFlagsByImpl: map[string]map[string]string{},
		VtSQLFlags:           map[string]string{},
		VtctldFlags:          map[string]string{},
	}

	if override.ID != "" {
		merged.ID = override.ID
	}

	if override.Name != "" {
		merged.Name = override.Name
	}

	if override.DiscoveryImpl != "" {
		merged.DiscoveryImpl = override.DiscoveryImpl
	}

	// first, the default flags
	merged.DiscoveryFlagsByImpl.Merge(cfg.DiscoveryFlagsByImpl)
	// then, apply any overrides
	merged.DiscoveryFlagsByImpl.Merge(override.DiscoveryFlagsByImpl)

	mergeStringMap(merged.VtSQLFlags, cfg.VtSQLFlags)
	mergeStringMap(merged.VtSQLFlags, override.VtSQLFlags)

	mergeStringMap(merged.VtctldFlags, cfg.VtctldFlags)
	mergeStringMap(merged.VtctldFlags, override.VtctldFlags)

	return merged
}

func mergeStringMap(base map[string]string, override map[string]string) {
	for k, v := range override {
		base[k] = v
	}
}
