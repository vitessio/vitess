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
	"encoding/json"
	"fmt"
	"maps"
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
	TabletTypesStr          string

	VStreamPacketSize                      int
	VStreamPacketSizeOverride              bool
	VStreamDynamicPacketSize               bool
	VStreamDynamicPacketSizeOverride       bool
	VStreamBinlogRotationThreshold         int64
	VStreamBinlogRotationThresholdOverride bool

	Overrides map[string]string
}

var configMutex sync.Mutex
var DefaultVReplicationConfig *VReplicationConfig

func GetVReplicationConfigDefaults(useCached bool) *VReplicationConfig {
	configMutex.Lock()
	defer configMutex.Unlock()
	if useCached && DefaultVReplicationConfig != nil {
		return DefaultVReplicationConfig
	}
	DefaultVReplicationConfig = &VReplicationConfig{
		ExperimentalFlags:       vreplicationExperimentalFlags,
		NetReadTimeout:          vreplicationNetReadTimeout,
		NetWriteTimeout:         vreplicationNetWriteTimeout,
		CopyPhaseDuration:       vreplicationCopyPhaseDuration,
		RetryDelay:              vreplicationRetryDelay,
		MaxTimeToRetryError:     vreplicationMaxTimeToRetryError,
		RelayLogMaxSize:         vreplicationRelayLogMaxSize,
		RelayLogMaxItems:        vreplicationRelayLogMaxItems,
		ReplicaLagTolerance:     vreplicationReplicaLagTolerance,
		HeartbeatUpdateInterval: vreplicationHeartbeatUpdateInterval,
		StoreCompressedGTID:     vreplicationStoreCompressedGTID,
		ParallelInsertWorkers:   vreplicationParallelInsertWorkers,
		TabletTypesStr:          vreplicationTabletTypesStr,

		VStreamPacketSizeOverride:              false,
		VStreamPacketSize:                      VStreamerDefaultPacketSize,
		VStreamDynamicPacketSizeOverride:       false,
		VStreamDynamicPacketSize:               VStreamerUseDynamicPacketSize,
		VStreamBinlogRotationThresholdOverride: false,
		VStreamBinlogRotationThreshold:         VStreamerBinlogRotationThreshold,

		Overrides: make(map[string]string),
	}
	return DefaultVReplicationConfig
}

func InitVReplicationConfigDefaults() *VReplicationConfig {
	return GetVReplicationConfigDefaults(true)
}

func NewVReplicationConfig(overrides map[string]string) (*VReplicationConfig, error) {
	InitVReplicationConfigDefaults()

	configMutex.Lock()
	c := &VReplicationConfig{}
	*c = *DefaultVReplicationConfig
	configMutex.Unlock()

	c.Overrides = maps.Clone(overrides)
	var errors []string
	getError := func(k, v string) string {
		return fmt.Sprintf("invalid value for %s: %s", k, v)
	}
	for k, v := range overrides {
		if v == "" {
			continue
		}
		switch k {
		case "vreplication_experimental_flags":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.ExperimentalFlags = value
			}
		case "vreplication_net_read_timeout":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.NetReadTimeout = int(value)
			}
		case "vreplication_net_write_timeout":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.NetWriteTimeout = int(value)
			}
		case "vreplication_copy_phase_duration":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.CopyPhaseDuration = value
			}
		case "vreplication_retry_delay":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.RetryDelay = value
			}
		case "vreplication_max_time_to_retry_on_error":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.MaxTimeToRetryError = value
			}
		case "relay_log_max_size":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.RelayLogMaxSize = int(value)
			}
		case "relay_log_max_items":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.RelayLogMaxItems = int(value)
			}
		case "vreplication_replica_lag_tolerance":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.ReplicaLagTolerance = value
			}
		case "vreplication_heartbeat_update_interval":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.HeartbeatUpdateInterval = int(value)
			}
		case "vreplication_store_compressed_gtid":
			value, err := strconv.ParseBool(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.StoreCompressedGTID = value
			}
		case "vreplication-parallel-insert-workers":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.ParallelInsertWorkers = int(value)
			}
		case "vstream_packet_size":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.VStreamPacketSizeOverride = true
				c.VStreamPacketSize = int(value)
			}
		case "vstream_dynamic_packet_size":
			value, err := strconv.ParseBool(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.VStreamDynamicPacketSizeOverride = true
				c.VStreamDynamicPacketSize = value
			}
		case "vstream_binlog_rotation_threshold":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.VStreamBinlogRotationThresholdOverride = true
				c.VStreamBinlogRotationThreshold = value
			}
		default:
			errors = append(errors, fmt.Sprintf("unknown vreplication config flag: %s", k))
		}
	}
	if len(errors) > 0 {
		return c, fmt.Errorf("%s", strings.Join(errors, ", "))
	}
	return c, nil
}

func (c VReplicationConfig) Map() map[string]string {
	return map[string]string{
		"vreplication_experimental_flags":         strconv.FormatInt(c.ExperimentalFlags, 10),
		"vreplication_net_read_timeout":           strconv.Itoa(c.NetReadTimeout),
		"vreplication_net_write_timeout":          strconv.Itoa(c.NetWriteTimeout),
		"vreplication_copy_phase_duration":        c.CopyPhaseDuration.String(),
		"vreplication_retry_delay":                c.RetryDelay.String(),
		"vreplication_max_time_to_retry_on_error": c.MaxTimeToRetryError.String(),
		"relay_log_max_size":                      strconv.Itoa(c.RelayLogMaxSize),
		"relay_log_max_items":                     strconv.Itoa(c.RelayLogMaxItems),
		"vreplication_replica_lag_tolerance":      c.ReplicaLagTolerance.String(),
		"vreplication_heartbeat_update_interval":  strconv.Itoa(c.HeartbeatUpdateInterval),
		"vreplication_store_compressed_gtid":      strconv.FormatBool(c.StoreCompressedGTID),
		"vreplication-parallel-insert-workers":    strconv.Itoa(c.ParallelInsertWorkers),
		"vstream_packet_size":                     strconv.Itoa(c.VStreamPacketSize),
		"vstream_dynamic_packet_size":             strconv.FormatBool(c.VStreamDynamicPacketSize),
		"vstream_binlog_rotation_threshold":       strconv.FormatInt(c.VStreamBinlogRotationThreshold, 10),
	}
}

func (c VReplicationConfig) String() string {
	s, _ := json.Marshal(c.Map())
	return string(s)
}
