/*
Copyright 2022 The Vitess Authors.
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

package mysql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/capabilities"
)

func TestServerVersionCapableOf(t *testing.T) {
	testcases := []struct {
		version    string
		capability capabilities.FlavorCapability
		isCapable  bool
	}{
		{
			version:    "8.0.14",
			capability: capabilities.InstantDDLFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.20",
			capability: capabilities.TransactionalGtidExecutedFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.0",
			capability: capabilities.InstantAddLastColumnFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.0",
			capability: capabilities.InstantAddDropColumnFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.7.29",
			capability: capabilities.TransactionalGtidExecutedFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.30",
			capability: capabilities.DynamicRedoLogCapacityFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.29",
			capability: capabilities.DynamicRedoLogCapacityFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.7.38",
			capability: capabilities.DynamicRedoLogCapacityFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.21",
			capability: capabilities.DisableRedoLogFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.20",
			capability: capabilities.DisableRedoLogFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.15",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.20",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.20-log",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  true,
		},
		{
			version:    "5.7.38",
			capability: capabilities.PerformanceSchemaDataLocksTableCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.20",
			capability: capabilities.PerformanceSchemaDataLocksTableCapability,
			isCapable:  true,
		}, {
			version:    "5.7.38",
			capability: capabilities.PerformanceSchemaMetadataLocksTableCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.20",
			capability: capabilities.PerformanceSchemaMetadataLocksTableCapability,
			isCapable:  true,
		},
		{
			// Some ridiculous version
			version:    "5914.234.17",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  true,
		},
		{
			// MySQL 9.0.0 should support modern capabilities
			version:    "9.0.0",
			capability: capabilities.InstantDDLFlavorCapability,
			isCapable:  true,
		},
		{
			// MySQL 9.1.0 should support transactional GTID executed
			version:    "9.1.0",
			capability: capabilities.TransactionalGtidExecutedFlavorCapability,
			isCapable:  true,
		},
		{
			// MySQL 9.0.5 should support check constraints
			version:    "9.0.5",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  true,
		},
		{
			// MySQL 9.2.1-log should support performance schema data locks
			version:    "9.2.1-log",
			capability: capabilities.PerformanceSchemaDataLocksTableCapability,
			isCapable:  true,
		},
	}
	for _, tc := range testcases {
		name := fmt.Sprintf("%s %v", tc.version, tc.capability)
		t.Run(name, func(t *testing.T) {
			capableOf := ServerVersionCapableOf(tc.version)
			isCapable, err := capableOf(tc.capability)
			assert.NoError(t, err)
			assert.Equal(t, tc.isCapable, isCapable)
		})
	}
}

func TestGetFlavor(t *testing.T) {
	testcases := []struct {
		version      string
		expectedType string
		description  string
	}{
		// MySQL 5.7.x versions should get mysqlFlavor57
		{
			version:      "5.7.29",
			expectedType: "mysqlFlavor57",
			description:  "MySQL 5.7 should use mysqlFlavor57",
		},
		{
			version:      "5.7.38-log",
			expectedType: "mysqlFlavor57",
			description:  "MySQL 5.7 with suffix should use mysqlFlavor57",
		},
		// MySQL 8.0.x versions with different capabilities
		{
			version:      "8.0.11",
			expectedType: "mysqlFlavor8Legacy",
			description:  "MySQL 8.0.11 should use mysqlFlavor8Legacy (no replica terminology)",
		},
		{
			version:      "8.0.22",
			expectedType: "mysqlFlavor8Legacy",
			description:  "MySQL 8.0.22 should use mysqlFlavor8Legacy (no replica terminology, < 8.0.26)",
		},
		{
			version:      "8.0.26",
			expectedType: "mysqlFlavor8",
			description:  "MySQL 8.0.26 should use mysqlFlavor8 (has replica terminology, < 8.2.0)",
		},
		{
			version:      "8.1.0",
			expectedType: "mysqlFlavor8",
			description:  "MySQL 8.1.0 should use mysqlFlavor8 (has replica terminology, < 8.2.0)",
		},
		{
			version:      "8.2.0",
			expectedType: "mysqlFlavor82",
			description:  "MySQL 8.2.0 should use mysqlFlavor82",
		},
		{
			version:      "8.3.0",
			expectedType: "mysqlFlavor82",
			description:  "MySQL 8.3.0 should use mysqlFlavor82",
		},
		{
			version:      "8.0.30-log",
			expectedType: "mysqlFlavor8",
			description:  "MySQL 8.0.30 with suffix should use mysqlFlavor8 (has replica terminology, < 8.2.0)",
		},
		// MySQL 9.x versions should get mysqlFlavor9
		{
			version:      "9.0.0",
			expectedType: "mysqlFlavor9",
			description:  "MySQL 9.0.0 should use mysqlFlavor9",
		},
		{
			version:      "9.1.0",
			expectedType: "mysqlFlavor9",
			description:  "MySQL 9.1.0 should use mysqlFlavor9",
		},
		{
			version:      "9.0.5-log",
			expectedType: "mysqlFlavor9",
			description:  "MySQL 9.0.5 with suffix should use mysqlFlavor9",
		},
		{
			version:      "9.2.1-debug",
			expectedType: "mysqlFlavor9",
			description:  "MySQL 9.2.1 with debug suffix should use mysqlFlavor9",
		},
		{
			version:      "9.10.15",
			expectedType: "mysqlFlavor9",
			description:  "MySQL 9.10.15 should use mysqlFlavor9",
		},
		// MariaDB versions
		{
			version:      "5.5.5-10.1.48-MariaDB",
			expectedType: "mariadbFlavor101",
			description:  "MariaDB 10.1 with replication hack prefix should use mariadbFlavor101",
		},
		{
			version:      "10.1.48-MariaDB",
			expectedType: "mariadbFlavor101",
			description:  "MariaDB 10.1 should use mariadbFlavor101",
		},
		{
			version:      "10.2.0-MariaDB",
			expectedType: "mariadbFlavor102",
			description:  "MariaDB 10.2 should use mariadbFlavor102",
		},
		{
			version:      "10.5.15-MariaDB-log",
			expectedType: "mariadbFlavor102",
			description:  "MariaDB 10.5 with suffix should use mariadbFlavor102",
		},
		// Default/unknown versions should get mysqlFlavor57
		{
			version:      "5.6.45",
			expectedType: "mysqlFlavor57",
			description:  "MySQL 5.6 should default to mysqlFlavor57",
		},
		{
			version:      "unknown-version",
			expectedType: "mysqlFlavor57",
			description:  "Unknown version should default to mysqlFlavor57",
		},
		{
			version:      "4.1.22",
			expectedType: "mysqlFlavor57",
			description:  "Very old MySQL version should default to mysqlFlavor57",
		},
	}

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%s_%s", tc.version, tc.expectedType), func(t *testing.T) {
			flavor, _, _ := GetFlavor(tc.version, nil)

			// Check the flavor type matches expected
			switch tc.expectedType {
			case "mysqlFlavor57":
				_, ok := flavor.(mysqlFlavor57)
				assert.True(t, ok, "Expected mysqlFlavor57 for version %s, but got %T. %s", tc.version, flavor, tc.description)
			case "mysqlFlavor8Legacy":
				_, ok := flavor.(mysqlFlavor8Legacy)
				assert.True(t, ok, "Expected mysqlFlavor8Legacy for version %s, but got %T. %s", tc.version, flavor, tc.description)
			case "mysqlFlavor8":
				_, ok := flavor.(mysqlFlavor8)
				assert.True(t, ok, "Expected mysqlFlavor8 for version %s, but got %T. %s", tc.version, flavor, tc.description)
			case "mysqlFlavor82":
				_, ok := flavor.(mysqlFlavor82)
				assert.True(t, ok, "Expected mysqlFlavor82 for version %s, but got %T. %s", tc.version, flavor, tc.description)
			case "mysqlFlavor9":
				_, ok := flavor.(mysqlFlavor9)
				assert.True(t, ok, "Expected mysqlFlavor9 for version %s, but got %T. %s", tc.version, flavor, tc.description)
			case "mariadbFlavor101":
				_, ok := flavor.(mariadbFlavor101)
				assert.True(t, ok, "Expected mariadbFlavor101 for version %s, but got %T. %s", tc.version, flavor, tc.description)
			case "mariadbFlavor102":
				_, ok := flavor.(mariadbFlavor102)
				assert.True(t, ok, "Expected mariadbFlavor102 for version %s, but got %T. %s", tc.version, flavor, tc.description)
			default:
				t.Errorf("Unknown expected type: %s", tc.expectedType)
			}
		})
	}
}
