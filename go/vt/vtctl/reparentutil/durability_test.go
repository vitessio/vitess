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

package reparentutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestSemiSyncACKersForPrimary(t *testing.T) {
	primaryTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  1,
		},
		Type: topodatapb.TabletType_PRIMARY,
	}
	replicaTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  2,
		},
		Type: topodatapb.TabletType_REPLICA,
	}
	rdonlyTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  3,
		},
		Type: topodatapb.TabletType_RDONLY,
	}
	replicaCrossCellTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-2",
			Uid:  2,
		},
		Type: topodatapb.TabletType_REPLICA,
	}
	rdonlyCrossCellTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-2",
			Uid:  3,
		},
		Type: topodatapb.TabletType_RDONLY,
	}
	tests := []struct {
		name               string
		durabilityPolicy   string
		primary            *topodatapb.Tablet
		allTablets         []*topodatapb.Tablet
		wantSemiSyncACKers []*topodatapb.Tablet
	}{
		{
			name:               "no other tablets",
			durabilityPolicy:   "none",
			primary:            primaryTablet,
			allTablets:         []*topodatapb.Tablet{primaryTablet},
			wantSemiSyncACKers: nil,
		}, {
			name:               "'none' durability policy",
			durabilityPolicy:   "none",
			primary:            primaryTablet,
			allTablets:         []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, replicaCrossCellTablet, rdonlyCrossCellTablet},
			wantSemiSyncACKers: nil,
		}, {
			name:               "'semi_sync' durability policy",
			durabilityPolicy:   "semi_sync",
			primary:            primaryTablet,
			allTablets:         []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, replicaCrossCellTablet, rdonlyCrossCellTablet},
			wantSemiSyncACKers: []*topodatapb.Tablet{replicaTablet, replicaCrossCellTablet},
		}, {
			name:               "'cross_cell' durability policy",
			durabilityPolicy:   "cross_cell",
			primary:            primaryTablet,
			allTablets:         []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, replicaCrossCellTablet, rdonlyCrossCellTablet},
			wantSemiSyncACKers: []*topodatapb.Tablet{replicaCrossCellTablet},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SetDurabilityPolicy(tt.durabilityPolicy, nil)
			require.NoError(t, err, "error setting durability policy")
			semiSyncACKers := SemiSyncACKersForPrimary(tt.primary, tt.allTablets)
			require.Equal(t, tt.wantSemiSyncACKers, semiSyncACKers)
		})
	}
}
