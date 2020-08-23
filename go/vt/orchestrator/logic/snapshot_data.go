/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package logic

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"

	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	orcraft "vitess.io/vitess/go/vt/orchestrator/raft"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

type SnapshotData struct {
	Keys             []inst.InstanceKey // Kept for backwards comapatibility
	MinimalInstances []inst.MinimalInstance
	RecoveryDisabled bool

	ClusterAlias,
	ClusterAliasOverride,
	ClusterDomainName,
	HostAttributes,
	InstanceTags,
	AccessToken,
	PoolInstances,
	InjectedPseudoGTIDClusters,
	HostnameResolves,
	HostnameUnresolves,
	DowntimedInstances,
	Candidates,
	Detections,
	KVStore,
	Recovery,
	RecoverySteps sqlutils.NamedResultData

	LeaderURI string
}

func NewSnapshotData() *SnapshotData {
	return &SnapshotData{}
}

func readTableData(tableName string, data *sqlutils.NamedResultData) error {
	orcdb, err := db.OpenOrchestrator()
	if err != nil {
		return log.Errore(err)
	}
	*data, err = sqlutils.ScanTable(orcdb, tableName)
	return log.Errore(err)
}

func writeTableData(tableName string, data *sqlutils.NamedResultData) error {
	orcdb, err := db.OpenOrchestrator()
	if err != nil {
		return log.Errore(err)
	}
	err = sqlutils.WriteTable(orcdb, tableName, *data)
	return log.Errore(err)
}

func CreateSnapshotData() *SnapshotData {
	snapshotData := NewSnapshotData()

	snapshotData.LeaderURI = orcraft.LeaderURI.Get()
	// keys
	snapshotData.Keys, _ = inst.ReadAllInstanceKeys()
	snapshotData.MinimalInstances, _ = inst.ReadAllMinimalInstances()
	snapshotData.RecoveryDisabled, _ = IsRecoveryDisabled()

	readTableData("cluster_alias", &snapshotData.ClusterAlias)
	readTableData("cluster_alias_override", &snapshotData.ClusterAliasOverride)
	readTableData("cluster_domain_name", &snapshotData.ClusterDomainName)
	readTableData("access_token", &snapshotData.AccessToken)
	readTableData("host_attributes", &snapshotData.HostAttributes)
	readTableData("database_instance_tags", &snapshotData.InstanceTags)
	readTableData("database_instance_pool", &snapshotData.PoolInstances)
	readTableData("hostname_resolve", &snapshotData.HostnameResolves)
	readTableData("hostname_unresolve", &snapshotData.HostnameUnresolves)
	readTableData("database_instance_downtime", &snapshotData.DowntimedInstances)
	readTableData("candidate_database_instance", &snapshotData.Candidates)
	readTableData("topology_failure_detection", &snapshotData.Detections)
	readTableData("kv_store", &snapshotData.KVStore)
	readTableData("topology_recovery", &snapshotData.Recovery)
	readTableData("topology_recovery_steps", &snapshotData.RecoverySteps)
	readTableData("cluster_injected_pseudo_gtid", &snapshotData.InjectedPseudoGTIDClusters)

	log.Debugf("raft snapshot data created")
	return snapshotData
}

type SnapshotDataCreatorApplier struct {
}

func NewSnapshotDataCreatorApplier() *SnapshotDataCreatorApplier {
	generator := &SnapshotDataCreatorApplier{}
	return generator
}

func (this *SnapshotDataCreatorApplier) GetData() (data []byte, err error) {
	snapshotData := CreateSnapshotData()
	b, err := json.Marshal(snapshotData)
	if err != nil {
		return b, err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return b, err
	}
	if err := zw.Close(); err != nil {
		return b, err
	}
	return buf.Bytes(), nil
}

func (this *SnapshotDataCreatorApplier) Restore(rc io.ReadCloser) error {
	snapshotData := NewSnapshotData()
	zr, err := gzip.NewReader(rc)
	if err != nil {
		return err
	}
	if err := json.NewDecoder(zr).Decode(&snapshotData); err != nil {
		return err
	}

	orcraft.LeaderURI.Set(snapshotData.LeaderURI)
	// keys
	{
		snapshotInstanceKeyMap := inst.NewInstanceKeyMap()
		snapshotInstanceKeyMap.AddKeys(snapshotData.Keys)
		for _, minimalInstance := range snapshotData.MinimalInstances {
			snapshotInstanceKeyMap.AddKey(minimalInstance.Key)
		}

		discardedKeys := 0
		// Forget instances that were not in snapshot
		existingKeys, _ := inst.ReadAllInstanceKeys()
		for _, existingKey := range existingKeys {
			if !snapshotInstanceKeyMap.HasKey(existingKey) {
				inst.ForgetInstance(&existingKey)
				discardedKeys++
			}
		}
		log.Debugf("raft snapshot restore: discarded %+v keys", discardedKeys)
		existingKeysMap := inst.NewInstanceKeyMap()
		existingKeysMap.AddKeys(existingKeys)

		// Discover instances that are in snapshot and not in our own database.
		// Instances that _are_ in our own database will self-discover. No need
		// to explicitly discover them.
		discoveredKeys := 0
		// v2: read keys + master keys
		for _, minimalInstance := range snapshotData.MinimalInstances {
			if !existingKeysMap.HasKey(minimalInstance.Key) {
				if err := inst.WriteInstance(minimalInstance.ToInstance(), false, nil); err == nil {
					discoveredKeys++
				} else {
					log.Errore(err)
				}
			}
		}
		if len(snapshotData.MinimalInstances) == 0 {
			// v1: read keys (backwards support)
			for _, snapshotKey := range snapshotData.Keys {
				if !existingKeysMap.HasKey(snapshotKey) {
					snapshotKey := snapshotKey
					go func() {
						snapshotDiscoveryKeys <- snapshotKey
					}()
					discoveredKeys++
				}
			}
		}
		log.Debugf("raft snapshot restore: discovered %+v keys", discoveredKeys)
	}
	writeTableData("cluster_alias", &snapshotData.ClusterAlias)
	writeTableData("cluster_alias_override", &snapshotData.ClusterAliasOverride)
	writeTableData("cluster_domain_name", &snapshotData.ClusterDomainName)
	writeTableData("access_token", &snapshotData.AccessToken)
	writeTableData("host_attributes", &snapshotData.HostAttributes)
	writeTableData("database_instance_tags", &snapshotData.InstanceTags)
	writeTableData("database_instance_pool", &snapshotData.PoolInstances)
	writeTableData("hostname_resolve", &snapshotData.HostnameResolves)
	writeTableData("hostname_unresolve", &snapshotData.HostnameUnresolves)
	writeTableData("database_instance_downtime", &snapshotData.DowntimedInstances)
	writeTableData("candidate_database_instance", &snapshotData.Candidates)
	writeTableData("kv_store", &snapshotData.KVStore)
	writeTableData("topology_recovery", &snapshotData.Recovery)
	writeTableData("topology_failure_detection", &snapshotData.Detections)
	writeTableData("topology_recovery_steps", &snapshotData.RecoverySteps)
	writeTableData("cluster_injected_pseudo_gtid", &snapshotData.InjectedPseudoGTIDClusters)

	// recovery disable
	{
		SetRecoveryDisabled(snapshotData.RecoveryDisabled)
	}
	log.Debugf("raft snapshot restore applied")
	return nil
}
