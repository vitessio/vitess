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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/utils"
)

func TestNewVReplicationConfig(t *testing.T) {
	InitVReplicationConfigDefaults()
	tests := []struct {
		name    string
		config  map[string]string
		wantErr int
		want    *VReplicationConfig
	}{
		{
			name: "Valid values",
			config: map[string]string{
				"vreplication-experimental-flags":                   "3",
				"vreplication-net-read-timeout":                     "100",
				"vreplication-net-write-timeout":                    "200",
				"vreplication-copy-phase-duration":                  "2h",
				"vreplication-retry-delay":                          "10s",
				"vreplication-max-time-to-retry-on-error":           "1h",
				utils.GetFlagVariantForTests("relay-log-max-size"):  "500000",
				utils.GetFlagVariantForTests("relay-log-max-items"): "10000",
				"vreplication-replica-lag-tolerance":                "2m",
				"vreplication-heartbeat-update-interval":            "2",
				"vreplication-store-compressed-gtid":                "true",
				"vreplication-parallel-insert-workers":              "4",
				"vstream-packet-size":                               "1024",
				"vstream_packet_size":                               "1024",
				"vstream-dynamic-packet-size":                       "false",
				"vstream_dynamic_packet_size":                       "false",
				"vstream_binlog_rotation_threshold":                 "2048",
			},
			wantErr: 0,
			want: &VReplicationConfig{
				ExperimentalFlags:                      3,
				NetReadTimeout:                         100,
				NetWriteTimeout:                        200,
				CopyPhaseDuration:                      2 * time.Hour,
				RetryDelay:                             10 * time.Second,
				MaxTimeToRetryError:                    1 * time.Hour,
				RelayLogMaxSize:                        500000,
				RelayLogMaxItems:                       10000,
				ReplicaLagTolerance:                    2 * time.Minute,
				HeartbeatUpdateInterval:                2,
				StoreCompressedGTID:                    true,
				ParallelInsertWorkers:                  4,
				VStreamPacketSize:                      1024,
				VStreamDynamicPacketSize:               false,
				VStreamBinlogRotationThreshold:         2048,
				TabletTypesStr:                         DefaultVReplicationConfig.TabletTypesStr,
				VStreamPacketSizeOverride:              true,
				VStreamDynamicPacketSizeOverride:       true,
				VStreamBinlogRotationThresholdOverride: true,
			},
		},
		{
			name: "Invalid values",
			config: map[string]string{
				"vreplication-experimental-flags":                   "invalid",
				"vreplication-net-read-timeout":                     "100.0",
				"vreplication-net-write-timeout":                    "invalid",
				"vreplication-copy-phase-duration":                  "invalid",
				"vreplication-retry-delay":                          "invalid",
				"vreplication-max-time-to-retry-on-error":           "invalid",
				utils.GetFlagVariantForTests("relay-log-max-size"):  "invalid",
				utils.GetFlagVariantForTests("relay-log-max-items"): "invalid",
				"vreplication-replica-lag-tolerance":                "invalid",
				"vreplication-heartbeat-update-interval":            "invalid",
				"vreplication-store-compressed-gtid":                "nottrue",
				"vreplication-parallel-insert-workers":              "invalid",
				"vstream-packet-size":                               "invalid",
				"vstream_packet_size":                               "invalid",
				"vstream-dynamic-packet-size":                       "waar",
				"vstream_dynamic_packet_size":                       "waar",
				"vstream_binlog_rotation_threshold":                 "invalid",
			},
			wantErr: 17,
		},
		{
			name: "Partial values",
			config: map[string]string{
				"vreplication-experimental-flags":    "5",
				"vreplication-net-read-timeout":      "150",
				"vstream-dynamic-packet-size":        strconv.FormatBool(!DefaultVReplicationConfig.VStreamDynamicPacketSize),
				"vreplication-store-compressed-gtid": strconv.FormatBool(!DefaultVReplicationConfig.StoreCompressedGTID),
			},
			wantErr: 0,
			want: &VReplicationConfig{
				ExperimentalFlags:                5,
				NetReadTimeout:                   150,
				NetWriteTimeout:                  DefaultVReplicationConfig.NetWriteTimeout,
				CopyPhaseDuration:                DefaultVReplicationConfig.CopyPhaseDuration,
				RetryDelay:                       DefaultVReplicationConfig.RetryDelay,
				MaxTimeToRetryError:              DefaultVReplicationConfig.MaxTimeToRetryError,
				RelayLogMaxSize:                  DefaultVReplicationConfig.RelayLogMaxSize,
				RelayLogMaxItems:                 DefaultVReplicationConfig.RelayLogMaxItems,
				ReplicaLagTolerance:              DefaultVReplicationConfig.ReplicaLagTolerance,
				HeartbeatUpdateInterval:          DefaultVReplicationConfig.HeartbeatUpdateInterval,
				StoreCompressedGTID:              !DefaultVReplicationConfig.StoreCompressedGTID,
				ParallelInsertWorkers:            DefaultVReplicationConfig.ParallelInsertWorkers,
				VStreamPacketSize:                DefaultVReplicationConfig.VStreamPacketSize,
				VStreamDynamicPacketSize:         !DefaultVReplicationConfig.VStreamDynamicPacketSize,
				VStreamBinlogRotationThreshold:   DefaultVReplicationConfig.VStreamBinlogRotationThreshold,
				VStreamDynamicPacketSizeOverride: true,
				TabletTypesStr:                   DefaultVReplicationConfig.TabletTypesStr,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitVReplicationConfigDefaults()
			got, err := NewVReplicationConfig(tt.config)
			if tt.wantErr == 0 {
				require.EqualValuesf(t, tt.config, got.Overrides,
					"NewVReplicationConfig() overrides got = %v, want %v", got.Overrides, tt.config)
			}
			if tt.wantErr > 0 && err == nil || tt.wantErr == 0 && err != nil {
				t.Errorf("NewVReplicationConfig() got num errors = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr > 0 && err != nil {
				errors := strings.Split(err.Error(), ", ")
				if len(errors) != tt.wantErr {
					t.Errorf("NewVReplicationConfig() got num errors = %v, want %v", len(errors), tt.wantErr)
				}
			}
			if tt.want == nil {
				require.EqualValuesf(t, DefaultVReplicationConfig.Map(), got.Map(),
					"NewVReplicationConfig() Map got = %v, want %v", got.Map(), DefaultVReplicationConfig.Map())
			} else {
				tt.want.Overrides = tt.config
				require.EqualValues(t, tt.want.Map(), got.Map(),
					"NewVReplicationConfig() Map got = %v, want %v", got.Map(), tt.want.Map())
			}
		})
	}
}
