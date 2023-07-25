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
	"fmt"
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
	byCharset   map[string]*colldefaults
	unsupported map[string]ID
}

// LookupByName returns the collation with the given name.
func (env *Environment) LookupByName(name string) Collation {
	return env.byName[name]
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
		return defaults.Default
	}
	return nil
}

// BinaryCollationForCharset returns the default binary collation for a charset
func (env *Environment) BinaryCollationForCharset(charset string) Collation {
	if defaults, ok := env.byCharset[charset]; ok {
		return defaults.Binary
	}
	return nil
}

// AllCollations returns a slice with all known collations in Vitess.
func (env *Environment) AllCollations() (all []Collation) {
	all = make([]Collation, 0, len(env.byName))
	for _, col := range env.byName {
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
	// 5.7 is the oldest version we support today, so use that as
	// the default.
	// NOTE: this should be changed when we EOL MySQL 5.7 support
	var version collver = collverMySQL57
	serverVersion = strings.TrimSpace(strings.ToLower(serverVersion))
	switch {
	case strings.HasSuffix(serverVersion, "-ripple"):
		// the ripple binlog server can mask the actual version of mysqld;
		// assume we have the highest
		version = collverMySQL80
	case strings.Contains(serverVersion, "mariadb"):
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
		byCharset:   make(map[string]*colldefaults),
		unsupported: make(map[string]ID),
	}

	for collid, vi := range globalVersionInfo {
		var ournames []string
		for _, alias := range vi.alias {
			if alias.mask&version != 0 {
				ournames = append(ournames, alias.name)
			}
		}
		if len(ournames) == 0 {
			continue
		}

		var collation Collation
		if int(collid) < len(collationsById) {
			collation = collationsById[collid]
		}
		if collation == nil {
			for _, name := range ournames {
				env.unsupported[name] = collid
			}
			continue
		}

		for _, name := range ournames {
			env.byName[name] = collation
		}

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

	for from, to := range version.charsetAliases() {
		env.byCharset[from] = env.byCharset[to]
	}

	return env
}

// A few interesting character set values.
// See http://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
const (
	CollationUtf8mb3ID     = 33
	CollationUtf8mb4ID     = 255
	CollationBinaryID      = 63
	CollationUtf8mb4BinID  = 46
	CollationLatin1Swedish = 8
)

// Binary is the default Binary collation
var Binary = ID(CollationBinaryID).Get()

// SystemCollation is the default collation for the system tables
// such as the information schema. This is still utf8mb3 to match
// MySQLs behavior. This means that you can't use utf8mb4 in table
// names, column names, without running into significant issues.
var SystemCollation = TypedCollation{
	Collation:    CollationUtf8mb3ID,
	Coercibility: CoerceCoercible,
	Repertoire:   RepertoireUnicode,
}

// CharsetAlias returns the internal charset name for the given charset.
// For now, this only maps `utf8` to `utf8mb3`; in future versions of MySQL,
// this mapping will change, so it's important to use this helper so that
// Vitess code has a consistent mapping for the active collations environment.
func (env *Environment) CharsetAlias(charset string) (alias string, ok bool) {
	alias, ok = env.version.charsetAliases()[charset]
	return
}

// CollationAlias returns the internal collaction name for the given charset.
// For now, this maps all `utf8` to `utf8mb3` collation names; in future versions of MySQL,
// this mapping will change, so it's important to use this helper so that
// Vitess code has a consistent mapping for the active collations environment.
func (env *Environment) CollationAlias(collation string) (string, bool) {
	col := env.LookupByName(collation)
	if col == nil {
		return collation, false
	}
	allCols, ok := globalVersionInfo[col.ID()]
	if !ok {
		return collation, false
	}
	if len(allCols.alias) == 1 {
		return collation, false
	}
	for _, alias := range allCols.alias {
		for source, dest := range env.version.charsetAliases() {
			if strings.HasPrefix(collation, fmt.Sprintf("%s_", source)) &&
				strings.HasPrefix(alias.name, fmt.Sprintf("%s_", dest)) {
				return alias.name, true
			}
		}
	}
	return collation, false
}

// DefaultConnectionCharset is the default charset that Vitess will use when negotiating a
// charset in a MySQL connection handshake. Note that in this context, a 'charset' is equivalent
// to a Collation ID, with the exception that it can only fit in 1 byte.
// For MySQL 8.0+ environments, the default charset is `utf8mb4_0900_ai_ci`.
// For older MySQL environments, the default charset is `utf8mb4_general_ci`.
func (env *Environment) DefaultConnectionCharset() uint8 {
	switch env.version {
	case collverMySQL80:
		return uint8(CollationUtf8mb4ID)
	default:
		return 45
	}
}

// ParseConnectionCharset parses the given charset name and returns its numerical
// identifier to be used in a MySQL connection handshake. The charset name can be:
// - the name of a character set, in which case the default collation ID for the
// character set is returned.
// - the name of a collation, in which case the ID for the collation is returned,
// UNLESS the collation itself has an ID greater than 255; such collations are not
// supported because they cannot be negotiated in a single byte in our connection
// handshake.
// - empty, in which case the default connection charset for this MySQL version
// is returned.
func (env *Environment) ParseConnectionCharset(csname string) (uint8, error) {
	if csname == "" {
		return env.DefaultConnectionCharset(), nil
	}

	var collid ID = 0
	csname = strings.ToLower(csname)
	if defaults, ok := env.byCharset[csname]; ok {
		collid = defaults.Default.ID()
	} else if coll, ok := env.byName[csname]; ok {
		collid = coll.ID()
	}
	if collid == 0 || collid > 255 {
		return 0, fmt.Errorf("unsupported connection charset: %q", csname)
	}
	return uint8(collid), nil
}
