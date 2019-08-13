/*
Copyright 2019 The Vitess Authors

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

type mysqlFlavor string

const (
	flavorMySQL   mysqlFlavor = "mysql"
	flavorPercona mysqlFlavor = "percona"
	flavorMariaDB mysqlFlavor = "mariadb"
)

// Mysqld is the object that represents a mysqld daemon running on this server.
type CapabilitySet struct {
	flavor  mysqlFlavor
	version serverVersion
}

func NewCapabilitySet(f mysqlFlavor, v serverVersion) (c CapabilitySet) {
	c.flavor = f
	c.version = v
	return
}

func (c *CapabilitySet) HasMySQLUpgradeInServer() bool {
	return c.IsMySQLLike() && c.version.atLeast(serverVersion{Major: 8, Minor: 0, Patch: 16})
}
func (c *CapabilitySet) HasInitializeInServer() bool {
	return c.IsMySQLLike() && c.version.atLeast(serverVersion{Major: 5, Minor: 7, Patch: 0})
}
func (c *CapabilitySet) HasMySQLxEnabledByDefault() bool {
	return c.IsMySQLLike() && c.version.atLeast(serverVersion{Major: 8, Minor: 0, Patch: 11})
}
func (c *CapabilitySet) HasPersistConfig() bool {
	return c.IsMySQLLike() && c.version.atLeast(serverVersion{Major: 8, Minor: 0, Patch: 0})
}
func (c *CapabilitySet) HasShutdownCommand() bool {
	return (c.IsMySQLLike() && c.version.atLeast(serverVersion{Major: 5, Minor: 7, Patch: 9})) || (c.IsMariaDB() && c.version.atLeast(serverVersion{Major: 10, Minor: 0, Patch: 4}))
}
func (c *CapabilitySet) HasBackupLocks() bool {
	return c.IsMySQLLike() && c.version.atLeast(serverVersion{Major: 8, Minor: 0, Patch: 0})
}
func (c *CapabilitySet) HasDefaultUft8mb4() bool {
	return c.IsMySQLLike() && c.version.atLeast(serverVersion{Major: 8, Minor: 0, Patch: 0})
}
func (c *CapabilitySet) HasSemiSyncEnabledByDefault() bool {
	return c.IsMariaDB() && c.version.atLeast(serverVersion{Major: 10, Minor: 3, Patch: 3})
}

// IsMySQLLike tests if the server is either MySQL
// or Percona Server. At least currently, Vitess doesn't
// make use of any specific Percona Server features.
func (c *CapabilitySet) IsMySQLLike() bool {
	return c.flavor == flavorMySQL || c.flavor == flavorPercona
}

// IsMariaDB tests if the server is MariaDB.
// IsMySQLLike() and IsMariaDB() are mutually exclusive
func (c *CapabilitySet) IsMariaDB() bool {
	return c.flavor == flavorMariaDB
}
