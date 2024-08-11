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

package vttablet

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type VReplicationConfig struct {
	ExperimentalFlags       int64
	NetReadTimeout          int
	NetWriteTimeout         int
	CopyPhaseDuration       time.Duration
	RetryDelay              time.Duration
	MaxTimeToRetryError     time.Duration
	RelayLogMaxSize         int
	RelayLogMaxItems        int
	ReplicaLagTolerance     time.Duration
	HeartbeatUpdateInterval int
	StoreCompressedGTID     bool
	ParallelInsertWorkers   int

	VStreamPacketSize                      int
	VStreamPacketSizeOverride              bool
	VStreamDynamicPacketSize               bool
	VStreamDynamicPacketSizeOverride       bool
	VStreamBinlogRotationThreshold         int64
	VStreamBinlogRotationThresholdOverride bool
}

var configMutex sync.Mutex
var DefaultVReplicationConfig *VReplicationConfig

func InitConfigDefaults() *VReplicationConfig {
	configMutex.Lock()
	defer configMutex.Unlock()
	if DefaultVReplicationConfig != nil {
		return DefaultVReplicationConfig
	}
	DefaultVReplicationConfig = &VReplicationConfig{
		ExperimentalFlags:       VReplicationExperimentalFlags,
		NetReadTimeout:          VReplicationNetReadTimeout,
		NetWriteTimeout:         VReplicationNetWriteTimeout,
		CopyPhaseDuration:       VReplicationCopyPhaseDuration,
		RetryDelay:              VReplicationRetryDelay,
		MaxTimeToRetryError:     VReplicationMaxTimeToRetryError,
		RelayLogMaxSize:         VReplicationRelayLogMaxSize,
		RelayLogMaxItems:        VReplicationRelayLogMaxItems,
		ReplicaLagTolerance:     VReplicationReplicaLagTolerance,
		HeartbeatUpdateInterval: VReplicationHeartbeatUpdateInterval,
		StoreCompressedGTID:     VReplicationStoreCompressedGTID,
		ParallelInsertWorkers:   VReplicationParallelInsertWorkers,

		VStreamPacketSizeOverride:              false,
		VStreamPacketSize:                      VStreamerDefaultPacketSize,
		VStreamDynamicPacketSizeOverride:       false,
		VStreamDynamicPacketSize:               VStreamerUseDynamicPacketSize,
		VStreamBinlogRotationThresholdOverride: false,
		VStreamBinlogRotationThreshold:         VStreamerBinlogRotationThreshold,
	}
	return DefaultVReplicationConfig
}

func NewVReplicationConfig(config map[string]string) (*VReplicationConfig, error) {
	configMutex.Lock()
	defer configMutex.Unlock()
	c := &VReplicationConfig{}
	*c = *DefaultVReplicationConfig
	var errors []string
	for k, v := range config {
		switch k {
		case "vreplication_experimental_flags":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, "invalid value for vreplication_experimental_flags")
			} else {
				c.ExperimentalFlags = value
			}
		case "vreplication_net_read_timeout":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, "invalid value for vreplication_net_read_timeout")
			} else {
				c.NetReadTimeout = int(value)
			}
		case "vreplication_net_write_timeout":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, "invalid value for vreplication_net_write_timeout")
			} else {
				c.NetWriteTimeout = int(value)
			}
		case "vreplication_copy_phase_duration":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, "invalid value for vreplication_copy_phase_duration")
			} else {
				c.CopyPhaseDuration = value
			}
		case "vreplication_retry_delay":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, "invalid value for vreplication_retry_delay")
			} else {
				c.RetryDelay = value
			}
		case "vreplication_max_time_to_retry_on_error":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, "invalid value for vreplication_max_time_to_retry_on_error")
			} else {
				c.MaxTimeToRetryError = value
			}
		case "relay_log_max_size":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, "invalid value for relay_log_max_size")
			} else {
				c.RelayLogMaxSize = int(value)
			}
		case "relay_log_max_items":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, "invalid value for relay_log_max_items")
			} else {
				c.RelayLogMaxItems = int(value)
			}
		case "vreplication_replica_lag_tolerance":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, "invalid value for vreplication_replica_lag_tolerance")
			} else {
				c.ReplicaLagTolerance = value
			}
		case "vreplication_heartbeat_update_interval":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, "invalid value for vreplication_heartbeat_update_interval")
			} else {
				c.HeartbeatUpdateInterval = int(value)
			}
		case "vreplication_store_compressed_gtid":
			value, err := strconv.ParseBool(v)
			if err != nil {
				errors = append(errors, "invalid value for vreplication_store_compressed_gtid")
			} else {
				c.StoreCompressedGTID = value
			}
		case "vreplication-parallel-insert-workers":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, "invalid value for vreplication-parallel-insert-workers")
			} else {
				c.ParallelInsertWorkers = int(value)
			}
		case "vstream_packet_size":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, "invalid value for vstream_packet_size")
			} else {
				c.VStreamPacketSizeOverride = true
				c.VStreamPacketSize = int(value)
			}
		case "vstream_dynamic_packet_size":
			value, err := strconv.ParseBool(v)
			if err != nil {
				errors = append(errors, "invalid value for vstream_dynamic_packet_size")
			} else {
				c.VStreamDynamicPacketSizeOverride = true
				c.VStreamDynamicPacketSize = value
			}
		case "vstream_binlog_rotation_threshold":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, "invalid value for vstream_binlog_rotation_threshold")
			} else {
				c.VStreamBinlogRotationThresholdOverride = true
				c.VStreamBinlogRotationThreshold = value
			}
		default:
			errors = append(errors, "unknown vreplication config flag: %s", k)
		}
	}
	if len(errors) > 0 {
		return nil, fmt.Errorf(strings.Join(errors, ", "))
	}
	return c, nil
}
