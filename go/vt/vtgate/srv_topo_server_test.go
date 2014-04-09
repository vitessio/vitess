// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/topo"
)

var cases = []struct {
	source *topo.EndPoints
	want   *topo.EndPoints
}{
	{
		source: nil,
		want:   nil,
	},
	{
		source: &topo.EndPoints{},
		want:   &topo.EndPoints{Entries: nil},
	},
	{
		source: &topo.EndPoints{Entries: []topo.EndPoint{}},
		want:   &topo.EndPoints{Entries: []topo.EndPoint{}},
	},
	{
		// all are healthy
		source: &topo.EndPoints{
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
						"Random": "Value1",
					},
				},
				topo.EndPoint{
					Uid:    4,
					Health: nil,
				},
			},
		},
		want: &topo.EndPoints{
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
						"Random": "Value1",
					},
				},
				topo.EndPoint{
					Uid:    4,
					Health: nil,
				},
			},
		},
	},
	{
		// 4 is unhealthy, it should be filtered out.
		source: &topo.EndPoints{
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
						"Random": "Value2",
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
		},
		want: &topo.EndPoints{
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
						"Random": "Value2",
					},
				},
				topo.EndPoint{
					Uid:    5,
					Health: nil,
				},
			},
		},
	},
	{
		// Only unhealthy servers, return all of them.
		source: &topo.EndPoints{
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
		},
		want: &topo.EndPoints{
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
		},
	},
}

func TestFilterUnhealthy(t *testing.T) {
	for _, c := range cases {
		if got := filterUnhealthyServers(c.source); !reflect.DeepEqual(got, c.want) {
			t.Errorf("filterUnhealthy(%+v)=%+v, want %+v", c.source, got, c.want)
		}
	}
}
