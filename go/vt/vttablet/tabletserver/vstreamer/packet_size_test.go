package vstreamer

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
		},
		{
			name:     "regression",
			baseSize: 25000,
			p:        polynomial{0.247, -0.726, 2.864, -3.022, 2.273, -0.641},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ps1 := newDynamicPacketSizer(tc.baseSize)
			elapsed1, sent1 := simulate(t, ps1, tc.baseSize, tc.baseSize*1000, tc.p.fit)

			ps2 := newFixedPacketSize(tc.baseSize)
			elapsed2, sent2 := simulate(t, ps2, tc.baseSize, tc.baseSize*1000, tc.p.fit)

			t.Logf("dynamic = (%v, %d), fixed = (%v, %d)", elapsed1, sent1, elapsed2, sent2)
			require.True(t, elapsed1 < elapsed2)
			require.True(t, sent1 < sent2)
		})
	}
}
