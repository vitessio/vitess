/*
Copyright 2021 The Vitess Authors.

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

package vstreamer

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

type polynomial []float64

func (p polynomial) fit(x float64) float64 {
	var y float64
	for i, exp := range p {
		y += exp * math.Pow(x, float64(i))
	}
	return y
}

func simulate(t *testing.T, ps PacketSizer, base, mustSend int, interpolate func(float64) float64) (time.Duration, int) {
	t.Helper()

	var elapsed time.Duration
	var sent int
	var sentPkt int
	packetRange := float64(base) * 10.0

	packetSize := 0
	for sent < mustSend {
		packetSize += rand.Intn(base / 100)

		if ps.ShouldSend(packetSize) {
			x := float64(packetSize) / packetRange
			y := interpolate(x)
			d := time.Duration(float64(time.Microsecond) * y * float64(packetSize))
			ps.Record(packetSize, d)

			sent += packetSize
			elapsed += d
			sentPkt++

			packetSize = 0
		}
	}
	return elapsed, sentPkt
}

func TestPacketSizeSimulation(t *testing.T) {
	cases := []struct {
		name     string
		baseSize int
		p        polynomial
		error    time.Duration
	}{
		{
			name:     "growth with tapper",
			baseSize: 25000,
			p:        polynomial{0.767, 1.278, -12.048, 25.262, -21.270, 6.410},
		},
		{
			name:     "growth without tapper",
			baseSize: 25000,
			p:        polynomial{0.473, 5.333, -38.663, 90.731, -87.005, 30.128},
			error:    5 * time.Millisecond,
		},
		{
			name:     "regression",
			baseSize: 25000,
			p:        polynomial{0.247, -0.726, 2.864, -3.022, 2.273, -0.641},
			error:    1 * time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seed := time.Now().UnixNano()
			rand.Seed(seed)

			// Simulate a replication using the given polynomial and the dynamic packet sizer
			ps1 := newDynamicPacketSizer(tc.baseSize)
			elapsed1, sent1 := simulate(t, ps1, tc.baseSize, tc.baseSize*1000, tc.p.fit)

			// Simulate the same polynomial using a fixed packet size
			ps2 := newFixedPacketSize(tc.baseSize)
			elapsed2, sent2 := simulate(t, ps2, tc.baseSize, tc.baseSize*1000, tc.p.fit)

			// the simulation for dynamic packet sizing should always be faster then the fixed packet,
			// and should also send fewer packets in total
			delta := elapsed1 - elapsed2
			if delta > tc.error {
				t.Errorf("packet-adjusted simulation is %v slower than fixed approach, seed %d", delta, seed)
			}
			if sent1 > sent2 {
				t.Errorf("packet-adjusted simulation sent more packets (%d) than fixed approach (%d), seed %d", sent1, sent2, seed)
			}
			// t.Logf("dynamic = (%v, %d), fixed = (%v, %d)", elapsed1, sent1, elapsed2, sent2)
		})
	}
}
