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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/vt/vtadmin/errors"
)

var (
	// DefaultRWPoolSize is the pool size used when creating read-write RPC
	// pools if a config has no size set.
	DefaultRWPoolSize = 50
	// DefaultReadPoolSize is the pool size used when creating read-only RPC
	// pools if a config has no size set.
	DefaultReadPoolSize = 500
	// DefaultRWPoolWaitTimeout is the pool wait timeout used when creating
	// read-write RPC pools if a config has no wait timeout set.
	DefaultRWPoolWaitTimeout = time.Millisecond * 100
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
	TopoRWPoolConfig       *RPCPoolConfig
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

func (cfg *Config) unmarshalMap(attributes map[string]string) error {
	for k, v := range attributes {
		if err := parseOne(cfg, k, v); err != nil {
			return err
		}
	}

	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (cfg *Config) MarshalJSON() ([]byte, error) {
	defaultRWPoolConfig := &RPCPoolConfig{
		Size:        DefaultRWPoolSize,
		WaitTimeout: DefaultRWPoolWaitTimeout,
	}
	defaultReadPoolConfig := &RPCPoolConfig{
		Size:        DefaultReadPoolSize,
		WaitTimeout: DefaultReadPoolWaitTimeout,
	}

	tmp := struct {
		ID                   string            `json:"id"`
		Name                 string            `json:"name"`
		DiscoveryImpl        string            `json:"discovery_impl"`
		DiscoveryFlagsByImpl FlagsByImpl       `json:"discovery_flags_by_impl"`
		TabletFQDNTmplStr    string            `json:"tablet_fqdn_tmpl_str"`
		VtSQLFlags           map[string]string `json:"vtsql_flags"`
		VtctldFlags          map[string]string `json:"vtctld_flags"`

		BackupReadPoolConfig   *RPCPoolConfig `json:"backup_read_pool_config"`
		SchemaReadPoolConfig   *RPCPoolConfig `json:"schema_read_pool_config"`
		TopoRWPoolConfig       *RPCPoolConfig `json:"topo_rw_pool_config"`
		TopoReadPoolConfig     *RPCPoolConfig `json:"topo_read_pool_config"`
		WorkflowReadPoolConfig *RPCPoolConfig `json:"workflow_read_pool_config"`
	}{
		ID:                     cfg.ID,
		Name:                   cfg.Name,
		DiscoveryImpl:          cfg.DiscoveryImpl,
		DiscoveryFlagsByImpl:   cfg.DiscoveryFlagsByImpl,
		VtSQLFlags:             cfg.VtSQLFlags,
		VtctldFlags:            cfg.VtctldFlags,
		BackupReadPoolConfig:   defaultReadPoolConfig.merge(cfg.BackupReadPoolConfig),
		SchemaReadPoolConfig:   defaultReadPoolConfig.merge(cfg.SchemaReadPoolConfig),
		TopoRWPoolConfig:       defaultRWPoolConfig.merge(cfg.TopoRWPoolConfig),
		TopoReadPoolConfig:     defaultReadPoolConfig.merge(cfg.TopoReadPoolConfig),
		WorkflowReadPoolConfig: defaultReadPoolConfig.merge(cfg.WorkflowReadPoolConfig),
	}

	return json.Marshal(&tmp)
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
		TopoRWPoolConfig:       cfg.TopoRWPoolConfig.merge(override.TopoRWPoolConfig),
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
	Size        int           `json:"size"`
	WaitTimeout time.Duration `json:"wait_timeout"`
}

// NewRWPool returns an RPCPool from the given config that should be used for
// performing read-write operations. If the config is nil, or has a non-positive
// size, DefaultRWPoolSize will be used. Similarly, if the config is nil or has
// a negative wait timeout, DefaultRWPoolWaitTimeout will be used.
func (cfg *RPCPoolConfig) NewRWPool() *pools.RPCPool {
	size := DefaultRWPoolSize
	waitTimeout := DefaultRWPoolWaitTimeout

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
