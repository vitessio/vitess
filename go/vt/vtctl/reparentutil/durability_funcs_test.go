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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	primaryTablet = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  1,
		},
		Type: topodatapb.TabletType_PRIMARY,
	}
	replicaTablet = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  2,
		},
		Type: topodatapb.TabletType_REPLICA,
	}
	rdonlyTablet = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  3,
		},
		Type: topodatapb.TabletType_RDONLY,
	}
	replicaCrossCellTablet = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-2",
			Uid:  2,
		},
		Type: topodatapb.TabletType_REPLICA,
	}
	rdonlyCrossCellTablet = &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-2",
			Uid:  3,
		},
		Type: topodatapb.TabletType_RDONLY,
	}
)

func TestSemiSyncAckersForPrimary(t *testing.T) {
	tests := []struct {
		name               string
		durabilityPolicy   string
		primary            *topodatapb.Tablet
		allTablets         []*topodatapb.Tablet
		wantSemiSyncAckers []*topodatapb.Tablet
	}{
		{
			name:               "no other tablets",
			durabilityPolicy:   "none",
			primary:            primaryTablet,
			allTablets:         []*topodatapb.Tablet{primaryTablet},
			wantSemiSyncAckers: nil,
		}, {
			name:               "'none' durability policy",
			durabilityPolicy:   "none",
			primary:            primaryTablet,
			allTablets:         []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, replicaCrossCellTablet, rdonlyCrossCellTablet},
			wantSemiSyncAckers: nil,
		}, {
			name:               "'semi_sync' durability policy",
			durabilityPolicy:   "semi_sync",
			primary:            primaryTablet,
			allTablets:         []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, replicaCrossCellTablet, rdonlyCrossCellTablet},
			wantSemiSyncAckers: []*topodatapb.Tablet{replicaTablet, replicaCrossCellTablet},
		}, {
			name:               "'cross_cell' durability policy",
			durabilityPolicy:   "cross_cell",
			primary:            primaryTablet,
			allTablets:         []*topodatapb.Tablet{primaryTablet, replicaTablet, rdonlyTablet, replicaCrossCellTablet, rdonlyCrossCellTablet},
			wantSemiSyncAckers: []*topodatapb.Tablet{replicaCrossCellTablet},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			durability, err := GetDurabilityPolicy(tt.durabilityPolicy)
			require.NoError(t, err, "error setting durability policy")
			semiSyncAckers := SemiSyncAckersForPrimary(durability, tt.primary, tt.allTablets)
			require.Equal(t, tt.wantSemiSyncAckers, semiSyncAckers)
		})
	}
}

func Test_haveRevokedForTablet(t *testing.T) {
	tests := []struct {
		name             string
		durabilityPolicy string
		primaryEligible  *topodatapb.Tablet
		allTablets       []*topodatapb.Tablet
		tabletsReached   []*topodatapb.Tablet
		revoked          bool
	}{
		{
			name:             "'none' durability policy - not revoked",
			durabilityPolicy: "none",
			primaryEligible:  primaryTablet,
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			tabletsReached: []*topodatapb.Tablet{
				replicaTablet, replicaCrossCellTablet, rdonlyTablet, rdonlyCrossCellTablet,
			},
			revoked: false,
		}, {
			name:             "'none' durability policy - revoked",
			durabilityPolicy: "none",
			primaryEligible:  primaryTablet,
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, rdonlyTablet, rdonlyCrossCellTablet,
			},
			revoked: true,
		}, {
			name:             "'semi_sync' durability policy - revoked",
			durabilityPolicy: "semi_sync",
			primaryEligible:  primaryTablet,
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			tabletsReached: []*topodatapb.Tablet{
				replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet,
			},
			revoked: true,
		}, {
			name:             "'semi_sync' durability policy - not revoked",
			durabilityPolicy: "semi_sync",
			primaryEligible:  primaryTablet,
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			tabletsReached: []*topodatapb.Tablet{
				replicaTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: false,
		}, {
			name:             "'cross_cell' durability policy - revoked",
			durabilityPolicy: "cross_cell",
			primaryEligible:  primaryTablet,
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			tabletsReached: []*topodatapb.Tablet{
				replicaCrossCellTablet,
			},
			revoked: true,
		}, {
			name:             "'cross_cell' durability policy - not revoked",
			durabilityPolicy: "cross_cell",
			primaryEligible:  primaryTablet,
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			tabletsReached: []*topodatapb.Tablet{
				replicaTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: false,
		}, {
			name:             "'cross_cell' durability policy - primary in list",
			durabilityPolicy: "cross_cell",
			primaryEligible:  primaryTablet,
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet,
			},
			revoked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			durability, err := GetDurabilityPolicy(tt.durabilityPolicy)
			require.NoError(t, err)
			out := haveRevokedForTablet(durability, tt.primaryEligible, tt.tabletsReached, tt.allTablets)
			require.Equal(t, tt.revoked, out)
		})
	}
}

func Test_haveRevoked(t *testing.T) {
	tests := []struct {
		name             string
		durabilityPolicy string
		tabletsReached   []*topodatapb.Tablet
		allTablets       []*topodatapb.Tablet
		revoked          bool
	}{
		{
			name:             "'none' durability policy - all tablets revoked",
			durabilityPolicy: "none",
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: true,
		}, {
			name:             "'semi_sync' durability policy - all tablets revoked",
			durabilityPolicy: "semi_sync",
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: true,
		}, {
			name:             "'cross_cell' durability policy - all tablets revoked",
			durabilityPolicy: "cross_cell",
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: true,
		}, {
			name:             "'none' durability policy - revoked",
			durabilityPolicy: "none",
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet,
			},
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: true,
		}, {
			name:             "'semi_sync' durability policy - revoked",
			durabilityPolicy: "semi_sync",
			tabletsReached: []*topodatapb.Tablet{
				replicaTablet, replicaCrossCellTablet, rdonlyTablet,
			},
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: true,
		}, {
			name:             "'cross_cell' durability policy - revoked",
			durabilityPolicy: "cross_cell",
			tabletsReached: []*topodatapb.Tablet{
				replicaCrossCellTablet,
			},
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: true,
		}, {
			name:             "'none' durability policy - not revoked",
			durabilityPolicy: "none",
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: false,
		}, {
			name:             "'semi_sync' durability policy - not revoked",
			durabilityPolicy: "semi_sync",
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: false,
		}, {
			name:             "'cross_cell' durability policy - not revoked",
			durabilityPolicy: "cross_cell",
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			allTablets: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			revoked: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			durability, err := GetDurabilityPolicy(tt.durabilityPolicy)
			require.NoError(t, err)
			out := haveRevoked(durability, tt.tabletsReached, tt.allTablets)
			require.Equal(t, tt.revoked, out)
		})
	}
}

func Test_canEstablishForTablet(t *testing.T) {
	tests := []struct {
		name             string
		durabilityPolicy string
		primaryEligible  *topodatapb.Tablet
		tabletsReached   []*topodatapb.Tablet
		canEstablish     bool
	}{
		{
			name:             "primary not reached",
			durabilityPolicy: "none",
			primaryEligible:  primaryTablet,
			tabletsReached: []*topodatapb.Tablet{
				replicaTablet, replicaCrossCellTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			canEstablish: false,
		}, {
			name:             "not established",
			durabilityPolicy: "semi_sync",
			primaryEligible:  primaryTablet,
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			canEstablish: false,
		}, {
			name:             "not established",
			durabilityPolicy: "cross_cell",
			primaryEligible:  primaryTablet,
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, replicaTablet, rdonlyCrossCellTablet, rdonlyTablet,
			},
			canEstablish: false,
		}, {
			name:             "established",
			durabilityPolicy: "none",
			primaryEligible:  primaryTablet,
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet,
			},
			canEstablish: true,
		}, {
			name:             "established",
			durabilityPolicy: "semi_sync",
			primaryEligible:  primaryTablet,
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, replicaTablet,
			},
			canEstablish: true,
		}, {
			name:             "established",
			durabilityPolicy: "cross_cell",
			primaryEligible:  primaryTablet,
			tabletsReached: []*topodatapb.Tablet{
				primaryTablet, replicaCrossCellTablet,
			},
			canEstablish: true,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("'%s' durability policy - %s", tt.durabilityPolicy, tt.name), func(t *testing.T) {
			durability, err := GetDurabilityPolicy(tt.durabilityPolicy)
			require.NoError(t, err)
			require.Equalf(t, tt.canEstablish, canEstablishForTablet(durability, tt.primaryEligible, tt.tabletsReached), "canEstablishForTablet(%v, %v)", tt.primaryEligible, tt.tabletsReached)
		})
	}
}
