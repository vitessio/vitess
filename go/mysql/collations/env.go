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

package collations

import (
	"strings"
	"sync"
)

type colldefaults struct {
	Default Collation
	Binary  Collation
}

// Environment is a collation environment for a MySQL version, which contains
// a database of collations and defaults for that specific version.
type Environment struct {
	version     collver
	byName      map[string]Collation
	byID        map[ID]Collation
	byCharset   map[string]*colldefaults
	unsupported map[string]ID
}

// LookupByName returns the collation with the given name. The collation
// is initialized if it's the first time being accessed.
func (env *Environment) LookupByName(name string) Collation {
	if coll, ok := env.byName[name]; ok {
		coll.Init()
		return coll
	}
	return nil
}

// LookupByID returns the collation with the given numerical identifier. The collation
// is initialized if it's the first time being accessed.
func (env *Environment) LookupByID(id ID) Collation {
	if coll, ok := env.byID[id]; ok {
		coll.Init()
		return coll
	}
	return nil
}

// LookupID returns the collation ID for the given name, and whether
// the collation is supported by this package.
func (env *Environment) LookupID(name string) (ID, bool) {
	if supported, ok := env.byName[name]; ok {
		return supported.ID(), true
	}
	if unsupported, ok := env.unsupported[name]; ok {
		return unsupported, false
	}
	return Unknown, false
}

// DefaultCollationForCharset returns the default collation for a charset
func (env *Environment) DefaultCollationForCharset(charset string) Collation {
	if defaults, ok := env.byCharset[charset]; ok {
		if defaults.Default != nil {
			defaults.Default.Init()
			return defaults.Default
		}
	}
	return nil
}

// BinaryCollationForCharset returns the default binary collation for a charset
func (env *Environment) BinaryCollationForCharset(charset string) Collation {
	if defaults, ok := env.byCharset[charset]; ok {
		if defaults.Binary != nil {
			defaults.Binary.Init()
			return defaults.Binary
		}
	}
	return nil
}

// AllCollations returns a slice with all known collations in Vitess. This is an expensive call because
// it will initialize the internal state of all the collations before returning them.
// Used for testing/debugging.
func (env *Environment) AllCollations() (all []Collation) {
	all = make([]Collation, 0, len(env.byID))
	for _, col := range env.byID {
		col.Init()
		all = append(all, col)
	}
	return
}

var globalEnvironments = make(map[collver]*Environment)
var globalEnvironmentsMu sync.Mutex

// fetchCacheEnvironment returns a cached Environment from a global cache.
// We can keep a single Environment per collver version because Environment
// objects are immutable once constructed.
func fetchCacheEnvironment(version collver) *Environment {
	globalEnvironmentsMu.Lock()
	defer globalEnvironmentsMu.Unlock()

	var env *Environment
	if env = globalEnvironments[version]; env == nil {
		env = makeEnv(version)
		globalEnvironments[version] = env
	}
	return env
}

// NewEnvironment creates a collation Environment for the given MySQL version string.
// The version string must be in the format that is sent by the server as the version packet
// when opening a new MySQL connection
func NewEnvironment(serverVersion string) *Environment {
	var version collver = collverMySQL56
	switch {
	case strings.HasSuffix(serverVersion, "-ripple"):
		// the ripple binlog server can mask the actual version of mysqld;
		// assume we have the highest
		version = collverMySQL80
	case strings.Contains(serverVersion, "MariaDB"):
		switch {
		case strings.Contains(serverVersion, "10.0."):
			version = collverMariaDB100
		case strings.Contains(serverVersion, "10.1."):
			version = collverMariaDB101
		case strings.Contains(serverVersion, "10.2."):
			version = collverMariaDB102
		case strings.Contains(serverVersion, "10.3."):
			version = collverMariaDB103
		}
	case strings.HasPrefix(serverVersion, "5.6."):
		version = collverMySQL56
	case strings.HasPrefix(serverVersion, "5.7."):
		version = collverMySQL57
	case strings.HasPrefix(serverVersion, "8.0."):
		version = collverMySQL80
	}
	return fetchCacheEnvironment(version)
}

func makeEnv(version collver) *Environment {
	env := &Environment{
		version:     version,
		byName:      make(map[string]Collation),
		byID:        make(map[ID]Collation),
		byCharset:   make(map[string]*colldefaults),
		unsupported: make(map[string]ID),
	}

	for collid, vi := range globalVersionInfo {
		var ourname string
		for mask, name := range vi.alias {
			if mask&version != 0 {
				ourname = name
				break
			}
		}
		if ourname == "" {
			continue
		}

		collation, ok := globalAllCollations[collid]
		if !ok {
			env.unsupported[ourname] = collid
			continue
		}

		env.byName[ourname] = collation
		env.byID[collid] = collation

		csname := collation.Charset().Name()
		if _, ok := env.byCharset[csname]; !ok {
			env.byCharset[csname] = &colldefaults{}
		}
		defaults := env.byCharset[csname]
		if vi.isdefault&version != 0 {
			defaults.Default = collation
		}
		if collation.IsBinary() {
			if defaults.Binary != nil && defaults.Binary.ID() > collation.ID() {
				// If there's more than one binary collation, the one with the
				// highest ID (i.e. the newest one) takes precedence. This applies
				// to utf8mb4_bin vs utf8mb4_0900_bin
				continue
			}
			defaults.Binary = collation
		}
	}
	return env
}

// Default is the default collation Environment for Vitess. This is set to
// the collation set and defaults available in MySQL 8.0
func Default() *Environment {
	return fetchCacheEnvironment(collverMySQL80)
}
