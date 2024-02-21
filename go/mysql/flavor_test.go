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
			version:    "5.6.7",
			capability: capabilities.InstantDDLFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.7.29",
			capability: capabilities.TransactionalGtidExecutedFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.6.7",
			capability: capabilities.MySQLJSONFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.7.29",
			capability: capabilities.MySQLJSONFlavorCapability,
			isCapable:  true,
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
		},
		{
			// What happens if server version is unspecified
			version:    "",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  false,
		},
		{
			// Some ridiculous version
			version:    "5914.234.17",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  false,
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
