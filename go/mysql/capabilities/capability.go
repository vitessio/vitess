/*
Copyright 2024 The Vitess Authors.

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

package capabilities

import (
	"strconv"
	"strings"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	ErrUnspecifiedServerVersion = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "server version unspecified")
)

type FlavorCapability int

const (
	NoneFlavorCapability                        FlavorCapability = iota // default placeholder
	FastDropTableFlavorCapability                                       // supported in MySQL 8.0.23 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-23.html
	TransactionalGtidExecutedFlavorCapability                           //
	InstantDDLFlavorCapability                                          // ALGORITHM=INSTANT general support
	InstantAddLastColumnFlavorCapability                                //
	InstantAddDropVirtualColumnFlavorCapability                         //
	InstantAddDropColumnFlavorCapability                                // Adding/dropping column in any position/ordinal.
	InstantChangeColumnDefaultFlavorCapability                          //
	InstantExpandEnumCapability                                         //
	MySQLJSONFlavorCapability                                           // JSON type supported
	MySQLUpgradeInServerFlavorCapability                                //
	DynamicRedoLogCapacityFlavorCapability                              // supported in MySQL 8.0.30 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-30.html
	DisableRedoLogFlavorCapability                                      // supported in MySQL 8.0.21 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-21.html
	CheckConstraintsCapability                                          // supported in MySQL 8.0.16 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-16.html
	PerformanceSchemaDataLocksTableCapability                           // supported in MySQL 8.0.1 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-1.html
	InstantDDLXtrabackupCapability                                      // Supported in 8.0.32 and above, solving a MySQL-vs-Xtrabackup bug starting 8.0.29
)

type CapableOf func(capability FlavorCapability) (bool, error)

// ServerVersionAtLeast returns true if current server is at least given value.
// Example: if input is []int{8, 0, 23}... the function returns 'true' if we're
// on MySQL 8.0.23, 8.0.24, ...
func ServerVersionAtLeast(serverVersion string, parts ...int) (bool, error) {
	if serverVersion == "" {
		return false, ErrUnspecifiedServerVersion
	}
	versionPrefix := strings.Split(serverVersion, "-")[0]
	versionTokens := strings.Split(versionPrefix, ".")
	for i, part := range parts {
		if len(versionTokens) <= i {
			return false, nil
		}
		tokenValue, err := strconv.Atoi(versionTokens[i])
		if err != nil {
			return false, err
		}
		if tokenValue > part {
			return true, nil
		}
		if tokenValue < part {
			return false, nil
		}
	}
	return true, nil
}

// MySQLVersionHasCapability is specific to MySQL flavors (of all versions) and answers whether
// the given server version has the requested capability.
func MySQLVersionHasCapability(serverVersion string, capability FlavorCapability) (bool, error) {
	atLeast := func(parts ...int) (bool, error) {
		return ServerVersionAtLeast(serverVersion, parts...)
	}
	// Capabilities sorted by version.
	switch capability {
	case MySQLJSONFlavorCapability:
		return atLeast(5, 7, 0)
	case InstantDDLFlavorCapability,
		InstantExpandEnumCapability,
		InstantAddLastColumnFlavorCapability,
		InstantAddDropVirtualColumnFlavorCapability,
		InstantChangeColumnDefaultFlavorCapability:
		return atLeast(8, 0, 0)
	case PerformanceSchemaDataLocksTableCapability:
		return atLeast(8, 0, 1)
	case MySQLUpgradeInServerFlavorCapability:
		return atLeast(8, 0, 16)
	case CheckConstraintsCapability:
		return atLeast(8, 0, 16)
	case TransactionalGtidExecutedFlavorCapability:
		return atLeast(8, 0, 17)
	case DisableRedoLogFlavorCapability:
		return atLeast(8, 0, 21)
	case FastDropTableFlavorCapability:
		return atLeast(8, 0, 23)
	case InstantAddDropColumnFlavorCapability:
		return atLeast(8, 0, 29)
	case DynamicRedoLogCapacityFlavorCapability:
		return atLeast(8, 0, 30)
	case InstantDDLXtrabackupCapability:
		return atLeast(8, 0, 32)
	default:
		return false, nil
	}
}

// MySQLVersionCapableOf returns a CapableOf function specific to MySQL flavors
func MySQLVersionCapableOf(serverVersion string) CapableOf {
	if serverVersion == "" {
		return nil
	}
	return func(capability FlavorCapability) (bool, error) {
		return MySQLVersionHasCapability(serverVersion, capability)
	}
}
