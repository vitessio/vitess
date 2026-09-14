/*
Copyright 2019 The Vitess Authors.

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

/*
 Detect server flavors and capabilities
*/

package mysqlctl

type MySQLFlavor string

// Flavor constants define the type of mysql flavor being used
const (
	FlavorMySQL   MySQLFlavor = "mysql"
	FlavorPercona MySQLFlavor = "percona"
	FlavorMariaDB MySQLFlavor = "mariadb"
	// FlavorUnknown is used when the flavor could not be determined, e.g. because
	// the version string was empty or failed to parse. It is a deliberate sentinel
	// distinct from the values ParseVersionString returns.
	FlavorUnknown MySQLFlavor = "unknown"
)

// FlavorFamily groups flavors that share a version lineage and replication
// compatibility, so their version numbers can be meaningfully compared.
type FlavorFamily string

const (
	// FlavorFamilyMySQL covers MySQL and Percona Server, which share the same
	// version numbering and binlog format and replicate bidirectionally at
	// matching versions.
	FlavorFamilyMySQL FlavorFamily = "mysql"
	// FlavorFamilyMariaDB covers MariaDB, which has its own version lineage
	// (10.x/11.x) and is not version-comparable with the MySQL family.
	FlavorFamilyMariaDB FlavorFamily = "mariadb"
	// FlavorFamilyUnknown covers flavors whose family could not be determined.
	FlavorFamilyUnknown FlavorFamily = "unknown"
)

// ReplicationFamily returns the compatibility family of the flavor. Flavors in
// the same family use the same version lineage, so their versions can be
// compared to reason about replication compatibility; flavors in different
// families cannot be meaningfully compared by version number.
func (f MySQLFlavor) ReplicationFamily() FlavorFamily {
	switch f {
	case FlavorMySQL, FlavorPercona:
		return FlavorFamilyMySQL
	case FlavorMariaDB:
		return FlavorFamilyMariaDB
	default:
		return FlavorFamilyUnknown
	}
}

// Mysqld is the object that represents a mysqld daemon running on this server.
type capabilitySet struct {
	flavor  MySQLFlavor
	version ServerVersion
}

func newCapabilitySet(f MySQLFlavor, v ServerVersion) (c capabilitySet) {
	noSocketFile()
	c.flavor = f
	c.version = v
	return
}

func (c *capabilitySet) hasMySQLUpgradeInServer() bool {
	return c.isMySQLLike() && c.version.atLeast(ServerVersion{Major: 8, Minor: 0, Patch: 16})
}

func (c *capabilitySet) hasInitializeInServer() bool {
	return c.isMySQLLike() && c.version.atLeast(ServerVersion{Major: 5, Minor: 7, Patch: 0})
}

func (c *capabilitySet) hasMaria104InstallDb() bool {
	return c.isMariaDB() && c.version.atLeast(ServerVersion{Major: 10, Minor: 4, Patch: 0})
}

// IsMySQLLike tests if the server is either MySQL
// or Percona Server. At least currently, Vitess doesn't
// make use of any specific Percona Server features.
func (c *capabilitySet) isMySQLLike() bool {
	return c.flavor == FlavorMySQL || c.flavor == FlavorPercona
}

func (c *capabilitySet) isMariaDB() bool {
	return c.flavor == FlavorMariaDB
}
