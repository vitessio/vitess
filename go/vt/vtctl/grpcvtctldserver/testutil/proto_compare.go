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

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// AssertKeyspacesEqual is a convenience function to assert that two
// vtctldatapb.Keyspace objects are equal, after clearing out any reserved
// proto XXX_ fields.
func AssertKeyspacesEqual(t *testing.T, expected *vtctldatapb.Keyspace, actual *vtctldatapb.Keyspace, msgAndArgs ...interface{}) {
	t.Helper()

	for _, ks := range []*vtctldatapb.Keyspace{expected, actual} {
		if ks.Keyspace != nil {
			ks.Keyspace.XXX_sizecache = 0
			ks.Keyspace.XXX_unrecognized = nil
		}

		if ks.Keyspace.SnapshotTime != nil {
			ks.Keyspace.SnapshotTime.XXX_sizecache = 0
			ks.Keyspace.SnapshotTime.XXX_unrecognized = nil
		}
	}

	assert.Equal(t, expected, actual, msgAndArgs...)
}
