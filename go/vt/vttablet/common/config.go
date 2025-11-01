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

/*
  This file contains the model for all the configuration parameters for VReplication workflows. It also provides methods to
  initialize the default configuration and to override the default configuration with user-provided values. The overrides
  are stored in the `config` sub-document of the `options` attribute in `_vt.vreplication` and merged with the defaults
  when the workflow is initialized.
*/

// VReplicationConfig has the all the configuration parameters for VReplication workflows, both applicable on the
// target (vreplication)and the source (vstreamer) side.
type VReplicationConfig struct {
	// Config parameters applicable to the target side (vreplication)
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
	EnableHttpLog           bool // Enable the /debug/vrlog endpoint

	// Config parameters applicable to the source side (vstreamer)
	// The coresponding Override fields are used to determine if the user has provided a value for the parameter so
	// that they can be sent in the VStreamer API calls to the source.
	VStreamPacketSize                      int
	VStreamPacketSizeOverride              bool
	VStreamDynamicPacketSize               bool
	VStreamDynamicPacketSizeOverride       bool
	VStreamBinlogRotationThreshold         int64
	VStreamBinlogRotationThresholdOverride bool

	// Overrides is a map of user-provided configuration values that override the default configuration.
	Overrides map[string]string
}

var configMutex sync.Mutex

// DefaultVReplicationConfig has the default values for VReplicationConfig initialized from the vttablet flags
// when the workflow is initialized.
var DefaultVReplicationConfig *VReplicationConfig

// GetVReplicationConfigDefaults returns the default VReplicationConfig. If `useCached` is true, it returns the previously
// loaded configuration. Otherwise it reloads the configuration from the vttablet flags. useCached is set to false
// when the vttablet flags are updated in unit tests.
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
		EnableHttpLog:           vreplicationEnableHttpLog,

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

// InitVReplicationConfigDefaults initializes the default VReplicationConfig in an idempotent way.
func InitVReplicationConfigDefaults() *VReplicationConfig {
	return GetVReplicationConfigDefaults(true)
}

// GetDefaultVReplicationConfig returns a copy of the default VReplicationConfig.
func GetDefaultVReplicationConfig() *VReplicationConfig {
	c := &VReplicationConfig{}
	defaultConfig := GetVReplicationConfigDefaults(true)
	*c = *defaultConfig
	return c
}

// NewVReplicationConfig creates a new VReplicationConfig by merging the default configuration with the user-provided
// overrides. It returns an error if the user-provided values are invalid.
func NewVReplicationConfig(overrides map[string]string) (*VReplicationConfig, error) {
	c := GetDefaultVReplicationConfig()
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
		case "vreplication-experimental-flags":
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.ExperimentalFlags = value
			}
		case "vreplication-net-read-timeout":
			value, err := strconv.Atoi(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.NetReadTimeout = value
			}
		case "vreplication-net-write-timeout":
			value, err := strconv.Atoi(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.NetWriteTimeout = value
			}
		case "vreplication-copy-phase-duration":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.CopyPhaseDuration = value
			}
		case "vreplication-retry-delay":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.RetryDelay = value
			}
		case "vreplication-max-time-to-retry-on-error":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.MaxTimeToRetryError = value
			}
		case "relay-log-max-size", "relay_log_max_size":
			value, err := strconv.Atoi(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.RelayLogMaxSize = value
			}
		case "relay-log-max-items", "relay_log_max_items":
			value, err := strconv.Atoi(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.RelayLogMaxItems = value
			}
		case "vreplication-replica-lag-tolerance":
			value, err := time.ParseDuration(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.ReplicaLagTolerance = value
			}
		case "vreplication-heartbeat-update-interval":
			value, err := strconv.Atoi(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.HeartbeatUpdateInterval = value
			}
		case "vreplication-store-compressed-gtid":
			value, err := strconv.ParseBool(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.StoreCompressedGTID = value
			}
		case "vreplication-parallel-insert-workers":
			value, err := strconv.Atoi(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.ParallelInsertWorkers = value
			}
		case "vstream-packet-size", "vstream_packet_size":
			value, err := strconv.Atoi(v)
			if err != nil {
				errors = append(errors, getError(k, v))
			} else {
				c.VStreamPacketSizeOverride = true
				c.VStreamPacketSize = value
			}
		case "vstream-dynamic-packet-size", "vstream_dynamic_packet_size":
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
			errors = append(errors, "unknown vreplication config flag: "+k)
		}
	}
	if len(errors) > 0 {
		return c, fmt.Errorf("%s", strings.Join(errors, ", "))
	}
	return c, nil
}

// Map returns a map of the VReplicationConfig: the keys are the flag names and the values are string representations.
// Used in tests to compare the expected and actual configuration values and in validations to check if the user-provided
// keys are one of those that are supported.
func (c VReplicationConfig) Map() map[string]string {
	return map[string]string{
		"vreplication-experimental-flags":         strconv.FormatInt(c.ExperimentalFlags, 10),
		"vreplication-net-read-timeout":           strconv.Itoa(c.NetReadTimeout),
		"vreplication-net-write-timeout":          strconv.Itoa(c.NetWriteTimeout),
		"vreplication-copy-phase-duration":        c.CopyPhaseDuration.String(),
		"vreplication-retry-delay":                c.RetryDelay.String(),
		"vreplication-max-time-to-retry-on-error": c.MaxTimeToRetryError.String(),
		"relay-log-max-size":                      strconv.Itoa(c.RelayLogMaxSize),
		"relay_log_max_size":                      strconv.Itoa(c.RelayLogMaxSize),
		"relay-log-max-items":                     strconv.Itoa(c.RelayLogMaxItems),
		"relay_log_max_items":                     strconv.Itoa(c.RelayLogMaxItems),
		"vreplication-replica-lag-tolerance":      c.ReplicaLagTolerance.String(),
		"vreplication-heartbeat-update-interval":  strconv.Itoa(c.HeartbeatUpdateInterval),
		"vreplication-store-compressed-gtid":      strconv.FormatBool(c.StoreCompressedGTID),
		"vreplication-parallel-insert-workers":    strconv.Itoa(c.ParallelInsertWorkers),
		"vstream-packet-size":                     strconv.Itoa(c.VStreamPacketSize),
		"vstream_packet_size":                     strconv.Itoa(c.VStreamPacketSize),
		"vstream-dynamic-packet-size":             strconv.FormatBool(c.VStreamDynamicPacketSize),
		"vstream_dynamic_packet_size":             strconv.FormatBool(c.VStreamDynamicPacketSize),
		"vstream_binlog_rotation_threshold":       strconv.FormatInt(c.VStreamBinlogRotationThreshold, 10),
	}
}

func (c VReplicationConfig) String() string {
	s, _ := json.Marshal(c.Map())
	return string(s)
}
