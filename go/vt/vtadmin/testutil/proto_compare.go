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

package testutil

import (
	"fmt"
	"sort"
	"testing"

	"vitess.io/vitess/go/test/utils"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// AssertKeyspaceSlicesEqual is a convenience function to assert that two
// []*vtadminpb.Keyspaces slices are equal
func AssertKeyspaceSlicesEqual(t *testing.T, expected []*vtadminpb.Keyspace, actual []*vtadminpb.Keyspace) {
	t.Helper()
	for _, ks := range [][]*vtadminpb.Keyspace{expected, actual} {
		for _, k := range ks {
			if k.Shards != nil {
				for _, ss := range k.Shards {
					ss.Shard.KeyRange = nil
				}
			}
		}
	}
	sort.Slice(expected, func(i, j int) bool {
		return fmt.Sprintf("%v", expected[i]) < fmt.Sprintf("%v", expected[j])
	})
	sort.Slice(actual, func(i, j int) bool {
		return fmt.Sprintf("%v", actual[i]) < fmt.Sprintf("%v", actual[j])
	})
	utils.MustMatch(t, expected, actual)
}

// AssertSchemaSlicesEqual is a convenience function to assert that two
// []*vtadminpb.Schema slices are equal
func AssertSchemaSlicesEqual(t *testing.T, expected []*vtadminpb.Schema, actual []*vtadminpb.Schema) {
	t.Helper()
	sort.Slice(expected, func(i, j int) bool {
		return fmt.Sprintf("%v", expected[i]) < fmt.Sprintf("%v", expected[j])
	})
	sort.Slice(actual, func(i, j int) bool {
		return fmt.Sprintf("%v", actual[i]) < fmt.Sprintf("%v", actual[j])
	})
	utils.MustMatch(t, expected, actual)
}

// AssertSrvVSchemaSlicesEqual is a convenience function to assert that two
// []*vtadminpb.SrvVSchema slices are equal
func AssertSrvVSchemaSlicesEqual(t *testing.T, expected []*vtadminpb.SrvVSchema, actual []*vtadminpb.SrvVSchema) {
	t.Helper()

	sort.Slice(expected, func(i, j int) bool {
		return fmt.Sprintf("%v", expected[i]) < fmt.Sprintf("%v", expected[j])
	})
	sort.Slice(actual, func(i, j int) bool {
		return fmt.Sprintf("%v", actual[i]) < fmt.Sprintf("%v", actual[j])
	})
	utils.MustMatch(t, expected, actual)
}

// AssertTabletSlicesEqual is a convenience function to assert that two
// []*vtadminpb.Tablet slices are equal
func AssertTabletSlicesEqual(t *testing.T, expected []*vtadminpb.Tablet, actual []*vtadminpb.Tablet) {
	t.Helper()
	sort.Slice(expected, func(i, j int) bool {
		return fmt.Sprintf("%v", expected[i]) < fmt.Sprintf("%v", expected[j])
	})
	sort.Slice(actual, func(i, j int) bool {
		return fmt.Sprintf("%v", actual[i]) < fmt.Sprintf("%v", actual[j])
	})
	utils.MustMatch(t, expected, actual)
}
