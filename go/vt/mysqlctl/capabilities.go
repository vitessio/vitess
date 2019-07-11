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

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	vtenv "vitess.io/vitess/go/vt/env"
	"vitess.io/vitess/go/vt/log"
)

type serverVersion []int

const (
	flavorMySQL   = "mysql"
	flavorPercona = "percona"
	flavorMariaDB = "mariadb"
)

const (
	CapabilityMySQLUpgradeInServer = 1 << iota
	CapabilityInitializeInServer
	CapabilityMySQLxEnabledByDefault
	CapabilityPersistConfig
	CapabilityShutdownCommand
	CapabilityBackupLocks
	CapabilityDefaultUtf8mb4
	CapabilitySemiSyncEnabledByDefault
	CapabilitySystemd // often package specific, not just version specific
)

var (
	minimumMySQLUpgradeInServer            = serverVersion{8, 0, 16}
	minimumMySQLInitializeInServer         = serverVersion{5, 7, 0}
	minimumMySQLxEnabledByDefault          = serverVersion{8, 0, 11}
	minimumMySQLPersistConfig              = serverVersion{8, 0, 0}
	minimumMySQLShutdownCommand            = serverVersion{5, 7, 9}
	minimumMySQLBackupLocks                = serverVersion{8, 0, 0}
	minimumMySQLDefaultUtf8mb4             = serverVersion{8, 0, 0}
	minimumMariaDBSemiSyncEnabledByDefault = serverVersion{10, 3, 3}
	minimumMariaDBShutdownCommand          = serverVersion{10, 0, 4}
)

// HasCapability will return a bool value of if the server
// has a particular feature, based on a known list of
// introduced versions and detection. This gets more
// complicated in MySQL 8.0 as several features were introduced
// after the initial GA release.
func (mysqld *Mysqld) HasCapability(capability int) bool {

	switch capability {
	case CapabilityMySQLUpgradeInServer:
		return mysqld.IsMySQLLike() && mysqld.greaterThan(minimumMySQLUpgradeInServer)
	case CapabilityInitializeInServer:
		return mysqld.IsMySQLLike() && mysqld.greaterThan(minimumMySQLInitializeInServer)
	case CapabilityMySQLxEnabledByDefault:
		return mysqld.IsMySQLLike() && mysqld.greaterThan(minimumMySQLxEnabledByDefault)
	case CapabilityPersistConfig:
		return mysqld.IsMySQLLike() && mysqld.greaterThan(minimumMySQLPersistConfig)
	case CapabilityShutdownCommand:
		return mysqld.IsMySQLLike() && mysqld.greaterThan(minimumMySQLShutdownCommand) || mysqld.IsMariaDB() && mysqld.greaterThan(minimumMariaDBShutdownCommand)
	case CapabilityBackupLocks:
		return mysqld.IsMySQLLike() && mysqld.greaterThan(minimumMySQLBackupLocks)
	case CapabilityDefaultUtf8mb4:
		return mysqld.IsMySQLLike() && mysqld.greaterThan(minimumMySQLDefaultUtf8mb4)
	case CapabilitySemiSyncEnabledByDefault:
		return mysqld.IsMariaDB() && mysqld.greaterThan(minimumMariaDBSemiSyncEnabledByDefault)
	case CapabilitySystemd:
		dir, err := vtenv.VtMysqlRoot()
		if err != nil {
			return false
		}
		_, err = binaryPath(dir, "mysqld_safe")
		return err != nil // if there is no mysqld_safe, it means this platform is systemd
	default:
		log.Warningf("%d: unknown capability", capability)
		return false
	}

}

func (mysqld *Mysqld) greaterThan(version serverVersion) bool {
	if mysqld.version[0] > version[0] {
		return true
	} else if mysqld.version[0] == version[0] && mysqld.version[1] > version[1] {
		return true
	} else if mysqld.version[0] == version[0] && mysqld.version[1] == version[1] && mysqld.version[2] >= version[2] {
		return true
	} else {
		return false
	}
}

// IsMySQLLike tests if the server is either MySQL
// or Percona Server. At least currently, Vitess doesn't
// make use of any specific Percona Server features.
func (mysqld *Mysqld) IsMySQLLike() bool {
	return mysqld.flavor == flavorMySQL || mysqld.flavor == flavorPercona
}

// IsMariaDB tests if the server is MariaDB.
// IsMySQLLike() and IsMariaDB() are mutually exclusive
func (mysqld *Mysqld) IsMariaDB() bool {
	return mysqld.flavor == flavorMariaDB
}

// MajorVersion returns an integer for the leading version.
// For example 8 in the case of MySQL 8.0.11, or
// 5 in the case of 5.6.15
func (mysqld *Mysqld) MajorVersion() int {
	return mysqld.version[0]
}

// MinorVersion returns an integer for the second version
// For example 6 in the case of 5.6.11
func (mysqld *Mysqld) MinorVersion() int {
	return mysqld.version[1]
}

// PatchVersion returns an integer for the third
// part of the version. For example 11 in the case of
// 5.6.11
func (mysqld *Mysqld) PatchVersion() int {
	return mysqld.version[2]
}

func (mysqld *Mysqld) getVersionString() (string, error) {

	mysqlRoot, err := vtenv.VtMysqlRoot()
	if err != nil {
		return "", err
	}
	mysqldPath, err := binaryPath(mysqlRoot, "mysqld")
	if err != nil {
		return "", err
	}

	_, version, err := execCmd(mysqldPath, []string{"--version"}, nil, mysqlRoot, nil)
	if err != nil {
		return "", err
	}

	return version, nil

}

func (mysqld *Mysqld) detectFlavor() (string, error) {

	version, err := mysqld.getVersionString()
	if err != nil {
		return version, err
	}

	// Check if the flavor has been defined
	// This takes first precedence.

	if strings.Contains(version, "MySQL Community Server") {
		return flavorMySQL, nil
	} else if strings.Contains(version, "Percona") {
		return flavorPercona, nil
	} else if strings.Contains(version, "MariaDB") {
		return flavorMariaDB, nil
	}

	return "", fmt.Errorf("Could not autodetect flavor!")

}

func (mysqld *Mysqld) detectVersion() (serverVersion, error) {

	version, err := mysqld.getVersionString()
	if err != nil {
		return nil, err
	}

	// Detect server version
	re := regexp.MustCompile(`Ver [0-9]+\.[0-9]+\.[0-9]+`)
	ver := re.Find([]byte(version))[4:]
	re = regexp.MustCompile(`[0-9]+`)
	v := re.FindAll([]byte(ver), -1)
	major, _ := strconv.Atoi(string(v[0]))
	minor, _ := strconv.Atoi(string(v[1]))
	patch, _ := strconv.Atoi(string(v[2]))

	return serverVersion{major, minor, patch}, nil

}
