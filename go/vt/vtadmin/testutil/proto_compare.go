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
	"testing"

	"github.com/stretchr/testify/assert"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// AssertKeyspaceSlicesEqual is a convenience function to assert that two
// []*vtadminpb.Keyspaces slices are equal, after clearing out any reserved
// proto XXX_ fields.
func AssertKeyspaceSlicesEqual(t *testing.T, expected []*vtadminpb.Keyspace, actual []*vtadminpb.Keyspace, msgAndArgs ...interface{}) {
	t.Helper()

	for _, ks := range [][]*vtadminpb.Keyspace{expected, actual} {
		for _, k := range ks {
			if k.Shards != nil {
				for _, ss := range k.Shards {
					ss.XXX_sizecache = 0
					ss.XXX_unrecognized = nil
					ss.Shard.KeyRange = nil
				}
			}
		}
	}

	assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// AssertSchemaSlicesEqual is a convenience function to assert that two
// []*vtadminpb.Schema slices are equal, after clearing out any reserved
// proto XXX_ fields.
func AssertSchemaSlicesEqual(t *testing.T, expected []*vtadminpb.Schema, actual []*vtadminpb.Schema, msgAndArgs ...interface{}) {
	t.Helper()

	for _, ss := range [][]*vtadminpb.Schema{expected, actual} {
		for _, s := range ss {
			if s.TableDefinitions != nil {
				for _, td := range s.TableDefinitions {
					td.XXX_sizecache = 0
					td.XXX_unrecognized = nil

					if td.Fields != nil {
						for _, f := range td.Fields {
							f.XXX_sizecache = 0
							f.XXX_unrecognized = nil
						}
					}
				}
			}
		}
	}

	assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// AssertTabletSlicesEqual is a convenience function to assert that two
// []*vtadminpb.Tablet slices are equal, after clearing out any reserved
// proto XXX_ fields.
func AssertTabletSlicesEqual(t *testing.T, expected []*vtadminpb.Tablet, actual []*vtadminpb.Tablet, msgAndArgs ...interface{}) {
	t.Helper()

	for _, ts := range [][]*vtadminpb.Tablet{expected, actual} {
		for _, t := range ts {
			t.XXX_sizecache = 0
			t.XXX_unrecognized = nil

			if t.Cluster != nil {
				t.Cluster.XXX_sizecache = 0
				t.Cluster.XXX_unrecognized = nil
			}

			if t.Tablet != nil {
				t.Tablet.XXX_sizecache = 0
				t.Tablet.XXX_unrecognized = nil

				if t.Tablet.Alias != nil {
					t.Tablet.Alias.XXX_sizecache = 0
					t.Tablet.Alias.XXX_unrecognized = nil
				}
			}
		}
	}

	assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// AssertTabletsEqual is a convenience function to assert that two
// *vtadminpb.Tablets are equal, after clearing out any reserved
// proto XXX_ fields.
func AssertTabletsEqual(t *testing.T, expected *vtadminpb.Tablet, actual *vtadminpb.Tablet, msgAndArgs ...interface{}) {
	t.Helper()

	AssertTabletSlicesEqual(t, []*vtadminpb.Tablet{expected}, []*vtadminpb.Tablet{actual}, msgAndArgs...)
}
