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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServerVersionAtLeast(t *testing.T) {
	testcases := []struct {
		version     string
		parts       []int
		expect      bool
		expectError bool
	}{
		{
			version: "8.0.14",
			parts:   []int{8, 0, 14},
			expect:  true,
		},
		{
			version: "8.0.14-log",
			parts:   []int{8, 0, 14},
			expect:  true,
		},
		{
			version: "8.0.14",
			parts:   []int{8, 0, 13},
			expect:  true,
		},
		{
			version: "8.0.14-log",
			parts:   []int{8, 0, 13},
			expect:  true,
		},
		{
			version: "8.0.14",
			parts:   []int{8, 0, 15},
			expect:  false,
		},
		{
			version: "8.0.14-log",
			parts:   []int{8, 0, 15},
			expect:  false,
		},
		{
			version: "8.0.14",
			parts:   []int{7, 5, 20},
			expect:  true,
		},
		{
			version: "8.0.14",
			parts:   []int{7, 5},
			expect:  true,
		},
		{
			version: "8.0.14",
			parts:   []int{5, 7},
			expect:  true,
		},
		{
			version: "8.0.14-log",
			parts:   []int{7, 5, 20},
			expect:  true,
		},
		{
			version: "8.0.14",
			parts:   []int{8, 1, 2},
			expect:  false,
		},
		{
			version: "8.0.14",
			parts:   []int{10, 1, 2},
			expect:  false,
		},
		{
			version: "8.0",
			parts:   []int{8, 0, 14},
			expect:  false,
		},
		{
			version:     "8.0.x",
			parts:       []int{8, 0, 14},
			expectError: true,
		},
		{
			version:     "",
			parts:       []int{8, 0, 14},
			expectError: true,
		},
	}
	for _, tc := range testcases {
		result, err := ServerVersionAtLeast(tc.version, tc.parts...)
		if tc.expectError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expect, result)
		}
	}
}

func TestMySQLVersionCapableOf(t *testing.T) {
	testcases := []struct {
		version    string
		capability FlavorCapability
		isCapable  bool
		expectNil  bool
	}{
		{
			version:    "8.0.14",
			capability: InstantDDLFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.20",
			capability: TransactionalGtidExecutedFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.0",
			capability: InstantAddLastColumnFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.0",
			capability: InstantAddDropColumnFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.6.7",
			capability: InstantDDLFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.7.29",
			capability: TransactionalGtidExecutedFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.6.7",
			capability: MySQLJSONFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.7.29",
			capability: MySQLJSONFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.30",
			capability: DynamicRedoLogCapacityFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.29",
			capability: DynamicRedoLogCapacityFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.7.38",
			capability: DynamicRedoLogCapacityFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.21",
			capability: DisableRedoLogFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.20",
			capability: DisableRedoLogFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.15",
			capability: CheckConstraintsCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.15-log",
			capability: CheckConstraintsCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.20",
			capability: CheckConstraintsCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.20-log",
			capability: CheckConstraintsCapability,
			isCapable:  true,
		},
		{
			version:    "5.7.38",
			capability: PerformanceSchemaDataLocksTableCapability,
			isCapable:  false,
		},
		{
			version:    "8.0",
			capability: PerformanceSchemaDataLocksTableCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.0",
			capability: PerformanceSchemaDataLocksTableCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.20",
			capability: PerformanceSchemaDataLocksTableCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.29",
			capability: InstantDDLXtrabackupCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.32",
			capability: InstantDDLXtrabackupCapability,
			isCapable:  true,
		},
		{
			// What happens if server version is unspecified
			version:    "",
			capability: CheckConstraintsCapability,
			isCapable:  false,
			expectNil:  true,
		},
		{
			// Some ridiculous version. But seeing that we force the question on a MySQLVersionCapableOf
			// then this far futuristic version should actually work.
			version:    "5914.234.17",
			capability: CheckConstraintsCapability,
			isCapable:  true,
		},
	}
	for _, tc := range testcases {
		name := fmt.Sprintf("%s %v", tc.version, tc.capability)
		t.Run(name, func(t *testing.T) {
			capableOf := MySQLVersionCapableOf(tc.version)
			if tc.expectNil {
				assert.Nil(t, capableOf)
				return
			}
			isCapable, err := capableOf(tc.capability)
			assert.NoError(t, err)
			assert.Equal(t, tc.isCapable, isCapable)
		})
	}
}
