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
	"fmt"
	"strconv"
	"time"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/vt/vtadmin/errors"
)

var (
	// DefaultReadPoolSize is the pool size used when creating read-only RPC
	// pools if a config has no size set.
	DefaultReadPoolSize = 500
	// DefaultReadPoolWaitTimeout is the pool wait timeout used when creating
	// read-only RPC pools if a config has no wait timeout set.
	DefaultReadPoolWaitTimeout = time.Millisecond * 100
)

// Config represents the options to configure a vtadmin cluster.
type Config struct {
	ID                   string
	Name                 string
	DiscoveryImpl        string
	DiscoveryFlagsByImpl FlagsByImpl
	TabletFQDNTmplStr    string
	VtSQLFlags           map[string]string
	VtctldFlags          map[string]string

	BackupReadPoolConfig   *RPCPoolConfig
	SchemaReadPoolConfig   *RPCPoolConfig
	TopoReadPoolConfig     *RPCPoolConfig
	WorkflowReadPoolConfig *RPCPoolConfig
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
		ID:                     cfg.ID,
		Name:                   cfg.Name,
		DiscoveryImpl:          cfg.DiscoveryImpl,
		DiscoveryFlagsByImpl:   map[string]map[string]string{},
		TabletFQDNTmplStr:      cfg.TabletFQDNTmplStr,
		VtSQLFlags:             map[string]string{},
		VtctldFlags:            map[string]string{},
		BackupReadPoolConfig:   cfg.BackupReadPoolConfig.merge(override.BackupReadPoolConfig),
		SchemaReadPoolConfig:   cfg.SchemaReadPoolConfig.merge(override.SchemaReadPoolConfig),
		TopoReadPoolConfig:     cfg.TopoReadPoolConfig.merge(override.TopoReadPoolConfig),
		WorkflowReadPoolConfig: cfg.WorkflowReadPoolConfig.merge(override.WorkflowReadPoolConfig),
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

	if override.TabletFQDNTmplStr != "" {
		merged.TabletFQDNTmplStr = override.TabletFQDNTmplStr
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

// RPCPoolConfig holds configuration options for creating RPCPools.
type RPCPoolConfig struct {
	Size        int
	WaitTimeout time.Duration
}

// NewReadPool returns an RPCPool from the given config that should be used for
// performing read-only operations. If the config is nil, or has a non-positive
// size, DefaultReadPoolSize will be used. Similarly, if the config is nil or
// has a negative wait timeout, DefaultReadPoolWaitTimeout will be used.
func (cfg *RPCPoolConfig) NewReadPool() *pools.RPCPool {
	size := DefaultReadPoolSize
	waitTimeout := DefaultReadPoolWaitTimeout

	if cfg != nil {
		if cfg.Size > 0 {
			size = cfg.Size
		}

		if cfg.WaitTimeout >= 0 {
			waitTimeout = cfg.WaitTimeout
		}
	}

	return pools.NewRPCPool(size, waitTimeout, nil)
}

// merge merges two RPCPoolConfigs, returning the merged version. neither of the
// original configs is modified as a result of merging, and both can be nil.
func (cfg *RPCPoolConfig) merge(override *RPCPoolConfig) *RPCPoolConfig {
	if cfg == nil && override == nil {
		return nil
	}

	merged := &RPCPoolConfig{
		Size:        -1,
		WaitTimeout: -1,
	}

	for _, c := range []*RPCPoolConfig{cfg, override} { // First apply the base config, then any overrides.
		if c != nil {
			if c.Size >= 0 {
				merged.Size = c.Size
			}

			if c.WaitTimeout > 0 {
				merged.WaitTimeout = c.WaitTimeout
			}
		}
	}

	return merged
}

func (cfg *RPCPoolConfig) parseFlag(name string, val string) (err error) {
	switch name {
	case "size":
		size, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}

		if size < 0 {
			return fmt.Errorf("%w: pool size must be non-negative; got %d", strconv.ErrRange, size)
		}

		cfg.Size = int(size)
	case "timeout":
		cfg.WaitTimeout, err = time.ParseDuration(val)
		if err != nil {
			return err
		}
	default:
		return errors.ErrNoFlag
	}

	return nil
}
