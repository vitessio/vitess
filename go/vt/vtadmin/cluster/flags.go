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
	"regexp"
	"strings"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtadmin/cache"
)

// FlagsByImpl groups a set of flags by discovery implementation. Its mapping is
// impl_name=>flag=>value.
type FlagsByImpl map[string]map[string]string

// Merge applies the flags in the parameter to the receiver, conflicts are
// resolved in favor of the parameter and not the receiver.
func (base *FlagsByImpl) Merge(override map[string]map[string]string) {
	if (*base) == nil {
		*base = map[string]map[string]string{}
	}

	for impl, flags := range override {
		_, ok := (*base)[impl]
		if !ok {
			(*base)[impl] = map[string]string{}
		}

		for k, v := range flags {
			(*base)[impl][k] = v
		}
	}
}

// ClustersFlag implements flag.Value allowing multiple occurrences of a flag to
// be accumulated into a map.
type ClustersFlag map[string]Config

// String is part of the flag.Value interface.
func (cf *ClustersFlag) String() string {
	buf := strings.Builder{}

	buf.WriteString("[")

	i := 0

	for _, cfg := range *cf {
		buf.WriteString(cfg.String())

		if i < len(*cf)-1 {
			buf.WriteString(" ")
		}

		i++
	}

	buf.WriteString("]")

	return buf.String()
}

// Type is part of the pflag.Value interface.
func (cf *ClustersFlag) Type() string {
	return "cluster.ClustersFlag"
}

// Set is part of the flag.Value interface. It merges the parsed config into the
// map, allowing ClustersFlag to power a repeated flag. See (*Config).Set for
// details on flag parsing.
func (cf *ClustersFlag) Set(value string) error {
	if (*cf) == nil {
		(*cf) = map[string]Config{}
	}

	cfg := Config{
		DiscoveryFlagsByImpl: map[string]map[string]string{},
	}

	if err := parseFlag(&cfg, value); err != nil {
		return err
	}

	// Merge a potentially existing config for the same cluster ID.
	c, ok := (*cf)[cfg.ID]
	if !ok {
		// If we don't have an existing config, create an empty one to "merge"
		// into.
		c = Config{}
	}

	(*cf)[cfg.ID] = cfg.Merge(c)

	return nil
}

// nolint:gochecknoglobals
var discoveryFlagRegexp = regexp.MustCompile(`^discovery-(?P<impl>\w+)-(?P<flag>.+)$`)

func parseFlag(cfg *Config, value string) error {
	args := strings.Split(value, ",")
	for _, arg := range args {
		var (
			name string
			val  string
		)

		if strings.Contains(arg, "=") {
			parts := strings.Split(arg, "=")
			name = parts[0]
			val = strings.Join(parts[1:], "=")
		} else {
			name = arg
			val = "true"
		}

		if err := parseOne(cfg, name, val); err != nil {
			return err
		}
	}

	return nil
}

func parseOne(cfg *Config, name string, val string) error {
	switch name {
	case "id":
		cfg.ID = val
	case "name":
		cfg.Name = val
	case "discovery":
		cfg.DiscoveryImpl = val
	case "tablet-fqdn-tmpl":
		cfg.TabletFQDNTmplStr = val
	default:
		switch {
		case strings.HasPrefix(name, "vtsql-"):
			if cfg.VtSQLFlags == nil {
				cfg.VtSQLFlags = map[string]string{}
			}

			cfg.VtSQLFlags[strings.TrimPrefix(name, "vtsql-")] = val
		case strings.HasPrefix(name, "vtctld-"):
			if cfg.VtctldFlags == nil {
				cfg.VtctldFlags = map[string]string{}
			}

			cfg.VtctldFlags[strings.TrimPrefix(name, "vtctld-")] = val
		case strings.HasPrefix(name, "backup-read-pool-"):
			if cfg.BackupReadPoolConfig == nil {
				cfg.BackupReadPoolConfig = &RPCPoolConfig{
					Size:        -1,
					WaitTimeout: -1,
				}
			}

			if err := cfg.BackupReadPoolConfig.parseFlag(strings.TrimPrefix(name, "backup-read-pool-"), val); err != nil {
				return fmt.Errorf("error parsing %s: %w", name, err)
			}
		case strings.HasPrefix(name, "schema-read-pool-"):
			if cfg.SchemaReadPoolConfig == nil {
				cfg.SchemaReadPoolConfig = &RPCPoolConfig{
					Size:        -1,
					WaitTimeout: -1,
				}
			}

			if err := cfg.SchemaReadPoolConfig.parseFlag(strings.TrimPrefix(name, "schema-read-pool-"), val); err != nil {
				return fmt.Errorf("error parsing %s: %w", name, err)
			}
		case strings.HasPrefix(name, "topo-read-pool-"):
			if cfg.TopoReadPoolConfig == nil {
				cfg.TopoReadPoolConfig = &RPCPoolConfig{
					Size:        -1,
					WaitTimeout: -1,
				}
			}

			if err := cfg.TopoReadPoolConfig.parseFlag(strings.TrimPrefix(name, "topo-read-pool-"), val); err != nil {
				return fmt.Errorf("error parsing %s: %w", name, err)
			}
		case strings.HasPrefix(name, "topo-rw-pool-"):
			if cfg.TopoRWPoolConfig == nil {
				cfg.TopoRWPoolConfig = &RPCPoolConfig{
					Size:        -1,
					WaitTimeout: -1,
				}
			}

			if err := cfg.TopoRWPoolConfig.parseFlag(strings.TrimPrefix(name, "topo-rw-pool-"), val); err != nil {
				return fmt.Errorf("error parsing %s: %w", name, err)
			}
		case strings.HasPrefix(name, "workflow-read-pool-"):
			if cfg.WorkflowReadPoolConfig == nil {
				cfg.WorkflowReadPoolConfig = &RPCPoolConfig{
					Size:        -1,
					WaitTimeout: -1,
				}
			}

			if err := cfg.WorkflowReadPoolConfig.parseFlag(strings.TrimPrefix(name, "workflow-read-pool-"), val); err != nil {
				return fmt.Errorf("error parsing %s: %w", name, err)
			}
		case strings.HasPrefix(name, "emergency-failover-pool-"):
			if cfg.EmergencyFailoverPoolConfig == nil {
				cfg.EmergencyFailoverPoolConfig = &RPCPoolConfig{
					Size:        -1,
					WaitTimeout: -1,
				}
			}

			if err := cfg.EmergencyFailoverPoolConfig.parseFlag(strings.TrimPrefix(name, "emergency-failover-pool-"), val); err != nil {
				return fmt.Errorf("error parsing %s: %w", name, err)
			}
		case strings.HasPrefix(name, "failover-pool-"):
			if cfg.FailoverPoolConfig == nil {
				cfg.FailoverPoolConfig = &RPCPoolConfig{
					Size:        -1,
					WaitTimeout: -1,
				}
			}

			if err := cfg.FailoverPoolConfig.parseFlag(strings.TrimPrefix(name, "failover-pool-"), val); err != nil {
				return fmt.Errorf("error parsing %s: %w", name, err)
			}
		case strings.HasPrefix(name, "schema-cache-"):
			if cfg.SchemaCacheConfig == nil {
				cfg.SchemaCacheConfig = &cache.Config{
					DefaultExpiration:                -1,
					CleanupInterval:                  -1,
					BackfillRequestTTL:               -1,
					BackfillRequestDuplicateInterval: -1,
					BackfillQueueSize:                -1,
					BackfillEnqueueWaitTime:          -1,
				}
			}

			if err := parseCacheConfigFlag(cfg.SchemaCacheConfig, strings.TrimPrefix(name, "schema-cache-"), val); err != nil {
				return fmt.Errorf("error parsing %s: %w", name, err)
			}
		default:
			match := discoveryFlagRegexp.FindStringSubmatch(name)
			if match == nil {
				// not a discovery flag
				log.Warningf("Attempted to parse %q as a discovery flag, ignoring ...", name)
				return nil
			}

			var impl, flag string

			for i, g := range discoveryFlagRegexp.SubexpNames() {
				switch g {
				case "impl":
					impl = match[i]
				case "flag":
					flag = match[i]
				}
			}

			if cfg.DiscoveryFlagsByImpl == nil {
				cfg.DiscoveryFlagsByImpl = map[string]map[string]string{}
			}

			if cfg.DiscoveryFlagsByImpl[impl] == nil {
				cfg.DiscoveryFlagsByImpl[impl] = map[string]string{}
			}

			cfg.DiscoveryFlagsByImpl[impl][flag] = val
		}
	}

	return nil
}
