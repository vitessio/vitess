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

package inst

import (
	"errors"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// ErrKeyspaceNotFound is a fixed error message used when a keyspace is not found in the database.
var ErrKeyspaceNotFound = errors.New("keyspace not found")

// ReadKeyspace reads the vitess keyspace record.
func ReadKeyspace(keyspaceName string) (*topo.KeyspaceInfo, error) {
	if err := topo.ValidateKeyspaceName(keyspaceName); err != nil {
		return nil, err
	}

	query := `select
			keyspace_type,
			durability_policy,
			disable_emergency_reparent
		from
			vitess_keyspace
		where
			keyspace = ?`
	args := sqlutils.Args(keyspaceName)

	keyspace := &topo.KeyspaceInfo{
		Keyspace: &topodatapb.Keyspace{},
	}
	err := db.QueryVTOrc(query, args, func(row sqlutils.RowMap) error {
		keyspace.KeyspaceType = topodatapb.KeyspaceType(row.GetInt32("keyspace_type"))
		keyspace.DurabilityPolicy = row.GetString("durability_policy")
		keyspace.VtorcConfig = &topodatapb.VtorcConfig{
			DisableEmergencyReparent: row.GetBool("disable_emergency_reparent"),
		}
		keyspace.SetKeyspaceName(keyspaceName)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if keyspace.KeyspaceName() == "" {
		return nil, ErrKeyspaceNotFound
	}
	return keyspace, nil
}

// ReadERSDisabledKeyspaces returns a slice containing the names of
// keyspaces with EmergencyReparentShard disabled. This uses the
// index: disable_emergency_reparent_idx_vitess_keyspace.
func ReadERSDisabledKeyspaces() ([]string, error) {
	keyspaces := make([]string, 0)
	query := `SELECT
			keyspace
		FROM
			vitess_keyspace
		WHERE
			disable_emergency_reparent = 1`
	err := db.QueryVTOrc(query, nil, func(row sqlutils.RowMap) error {
		keyspaces = append(keyspaces, row.GetString("keyspace"))
		return nil
	})
	return keyspaces, err
}

// SaveKeyspace saves the keyspace record against the keyspace name.
func SaveKeyspace(keyspace *topo.KeyspaceInfo) error {
	var disableEmergencyReparent int
	if keyspace.VtorcConfig != nil && keyspace.VtorcConfig.DisableEmergencyReparent {
		disableEmergencyReparent = 1
	}
	_, err := db.ExecVTOrc(`
		replace	into vitess_keyspace (
			keyspace, keyspace_type, durability_policy, disable_emergency_reparent
		) values (
			?, ?, ?, ?
		)`,
		keyspace.KeyspaceName(),
		int(keyspace.KeyspaceType),
		keyspace.GetDurabilityPolicy(),
		disableEmergencyReparent,
	)
	return err
}

// GetDurabilityPolicy gets the durability policy for the given keyspace.
func GetDurabilityPolicy(keyspace string) (policy.Durabler, error) {
	ki, err := ReadKeyspace(keyspace)
	if err != nil {
		return nil, err
	}
	return policy.GetDurabilityPolicy(ki.DurabilityPolicy)
}
