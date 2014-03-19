// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"testing"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/topo"
)

func TestFilterUnhealthyServersCornerCases(t *testing.T) {
	{
		var source *topo.EndPoints
		result := filterUnhealthyServers(source)
		if result != nil {
			t.Fatalf("unexpected result: %v", result)
		}
	}
	{
		source := &topo.EndPoints{}
		result := filterUnhealthyServers(source)
		if result == nil || result.Entries != nil {
			t.Fatalf("unexpected result: %v", result)
		}
	}
	{
		source := &topo.EndPoints{Entries: []topo.EndPoint{}}
		result := filterUnhealthyServers(source)
		if result == nil || result.Entries == nil || len(result.Entries) != 0 {
			t.Fatalf("unexpected result: %v", result)
		}
	}
}

func TestFilterUnhealthyServersMixed(t *testing.T) {
	source := &topo.EndPoints{
		Entries: []topo.EndPoint{
			topo.EndPoint{
				Uid:    1,
				Health: nil,
			},
			topo.EndPoint{
				Uid:    2,
				Health: map[string]string{},
			},
			topo.EndPoint{
				Uid: 3,
				Health: map[string]string{
					"Random": "Value",
				},
			},
			topo.EndPoint{
				Uid: 4,
				Health: map[string]string{
					health.ReplicationLag: health.ReplicationLagHigh,
				},
			},
			topo.EndPoint{
				Uid:    5,
				Health: nil,
			},
		},
	}

	result := filterUnhealthyServers(source)
	if len(result.Entries) != 4 ||
		result.Entries[0].Uid != 1 ||
		result.Entries[1].Uid != 2 ||
		result.Entries[2].Uid != 3 ||
		result.Entries[3].Uid != 5 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestFilterUnhealthyServersAllHealthy(t *testing.T) {
	source := &topo.EndPoints{
		Entries: []topo.EndPoint{
			topo.EndPoint{
				Uid:    1,
				Health: nil,
			},
			topo.EndPoint{
				Uid:    2,
				Health: map[string]string{},
			},
			topo.EndPoint{
				Uid: 3,
				Health: map[string]string{
					"Random": "Value",
				},
			},
			topo.EndPoint{
				Uid:    4,
				Health: nil,
			},
		},
	}

	result := filterUnhealthyServers(source)
	if len(result.Entries) != 4 ||
		result.Entries[0].Uid != 1 ||
		result.Entries[1].Uid != 2 ||
		result.Entries[2].Uid != 3 ||
		result.Entries[3].Uid != 4 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestFilterUnhealthyServersAllUnhealthy(t *testing.T) {
	source := &topo.EndPoints{
		Entries: []topo.EndPoint{
			topo.EndPoint{
				Uid: 1,
				Health: map[string]string{
					health.ReplicationLag: health.ReplicationLagHigh,
				},
			},
			topo.EndPoint{
				Uid: 2,
				Health: map[string]string{
					health.ReplicationLag: health.ReplicationLagHigh,
				},
			},
			topo.EndPoint{
				Uid: 3,
				Health: map[string]string{
					health.ReplicationLag: health.ReplicationLagHigh,
				},
			},
		},
	}

	result := filterUnhealthyServers(source)
	if len(result.Entries) != 3 ||
		result.Entries[0].Uid != 1 ||
		result.Entries[1].Uid != 2 ||
		result.Entries[2].Uid != 3 {
		t.Fatalf("unexpected result: %v", result)
	}
}
