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
	"regexp"
	"strings"
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
	default:
		if strings.HasPrefix(name, "vtsql-") {
			if cfg.VtSQLFlags == nil {
				cfg.VtSQLFlags = map[string]string{}
			}

			cfg.VtSQLFlags[strings.TrimPrefix(name, "vtsql-")] = val

			return nil
		} else if strings.HasPrefix(name, "vtctld-") {
			if cfg.VtctldFlags == nil {
				cfg.VtctldFlags = map[string]string{}
			}

			cfg.VtctldFlags[strings.TrimPrefix(name, "vtctld-")] = val
		}

		match := discoveryFlagRegexp.FindStringSubmatch(name)
		if match == nil {
			// not a discovery flag
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

	return nil
}
