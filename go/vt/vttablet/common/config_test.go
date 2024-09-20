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
				"vreplication_experimental_flags":         "3",
				"vreplication_net_read_timeout":           "100",
				"vreplication_net_write_timeout":          "200",
				"vreplication_copy_phase_duration":        "2h",
				"vreplication_retry_delay":                "10s",
				"vreplication_max_time_to_retry_on_error": "1h",
				"relay_log_max_size":                      "500000",
				"relay_log_max_items":                     "10000",
				"vreplication_replica_lag_tolerance":      "2m",
				"vreplication_heartbeat_update_interval":  "2",
				"vreplication_store_compressed_gtid":      "true",
				"vreplication-parallel-insert-workers":    "4",
				"vstream_packet_size":                     "1024",
				"vstream_dynamic_packet_size":             "false",
				"vstream_binlog_rotation_threshold":       "2048",
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
				"vreplication_experimental_flags":         "invalid",
				"vreplication_net_read_timeout":           "100.0",
				"vreplication_net_write_timeout":          "invalid",
				"vreplication_copy_phase_duration":        "invalid",
				"vreplication_retry_delay":                "invalid",
				"vreplication_max_time_to_retry_on_error": "invalid",
				"relay_log_max_size":                      "invalid",
				"relay_log_max_items":                     "invalid",
				"vreplication_replica_lag_tolerance":      "invalid",
				"vreplication_heartbeat_update_interval":  "invalid",
				"vreplication_store_compressed_gtid":      "nottrue",
				"vreplication-parallel-insert-workers":    "invalid",
				"vstream_packet_size":                     "invalid",
				"vstream_dynamic_packet_size":             "waar",
				"vstream_binlog_rotation_threshold":       "invalid",
			},
			wantErr: 15,
		},
		{
			name: "Partial values",
			config: map[string]string{
				"vreplication_experimental_flags":    "5",
				"vreplication_net_read_timeout":      "150",
				"vstream_dynamic_packet_size":        strconv.FormatBool(!DefaultVReplicationConfig.VStreamDynamicPacketSize),
				"vreplication_store_compressed_gtid": strconv.FormatBool(!DefaultVReplicationConfig.StoreCompressedGTID),
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
