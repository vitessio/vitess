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
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/mathstats"
)

var (
	defaultPacketSize    = 250000
	useDynamicPacketSize = true
)

func init() {
	servenv.OnParseFor("vtcombo", registerPacketSizeFlags)
	servenv.OnParseFor("vttablet", registerPacketSizeFlags)
}

func registerPacketSizeFlags(fs *pflag.FlagSet) {
	// defaultPacketSize is the suggested packet size for VReplication streamer.
	fs.IntVar(&defaultPacketSize, "vstream_packet_size", defaultPacketSize, "Suggested packet size for VReplication streamer. This is used only as a recommendation. The actual packet size may be more or less than this amount.")
	// useDynamicPacketSize controls whether to use dynamic packet size adjustments to increase performance while streaming
	fs.BoolVar(&useDynamicPacketSize, "vstream_dynamic_packet_size", useDynamicPacketSize, "Enable dynamic packet sizing for VReplication. This will adjust the packet size during replication to improve performance.")
}

// PacketSizer is a controller that adjusts the size of the packets being sent by the vstreamer at runtime
type PacketSizer interface {
	ShouldSend(byteCount int) bool
	Record(byteCount int, duration time.Duration)
	Limit() int
}

// DefaultPacketSizer creates a new PacketSizer using the default settings.
// If dynamic packet sizing is enabled, this will return a dynamicPacketSizer.
func DefaultPacketSizer() PacketSizer {
	if useDynamicPacketSize {
		return newDynamicPacketSizer(defaultPacketSize)
	}
	return newFixedPacketSize(defaultPacketSize)
}

// AdjustPacketSize temporarily adjusts the default packet sizes to the given value.
// Calling the returned cleanup function resets them to their original value.
// This function is only used for testing.
func AdjustPacketSize(size int) func() {
	originalSize := defaultPacketSize
	originalDyn := useDynamicPacketSize

	defaultPacketSize = size
	useDynamicPacketSize = false

	return func() {
		defaultPacketSize = originalSize
		useDynamicPacketSize = originalDyn
	}
}

type fixedPacketSizer struct {
	baseSize int
}

func newFixedPacketSize(baseSize int) PacketSizer {
	return &fixedPacketSizer{baseSize: baseSize}
}

func (ps *fixedPacketSizer) Limit() int {
	return ps.baseSize
}

// ShouldSend checks whether the given byte count is large enough to be sent as a packet while streaming
func (ps *fixedPacketSizer) ShouldSend(byteCount int) bool {
	return byteCount >= ps.baseSize
}

// Record records the total duration it took to send the given byte count while streaming
func (ps *fixedPacketSizer) Record(_ int, _ time.Duration) {}

type dynamicPacketSizer struct {
	// currentSize is the last size for the packet that is safe to use
	currentSize int

	// currentMetrics are the performance metrics for the current size
	currentMetrics *mathstats.Sample

	// candidateSize is the target size for packets being tested
	candidateSize int

	// candidateMetrics are the performance metrics for this new metric
	candidateMetrics *mathstats.Sample

	// grow is the growth rate with which we adjust the packet size
	grow int

	// calls is the amount of calls to the packet sizer
	calls int

	// settled is true when the last experiment has finished and arrived at a new target packet size
	settled bool

	// elapsed is the time elapsed since the last experiment was settled
	elapsed time.Duration
}

func newDynamicPacketSizer(baseSize int) PacketSizer {
	return &dynamicPacketSizer{
		currentSize:      baseSize,
		currentMetrics:   &mathstats.Sample{},
		candidateMetrics: &mathstats.Sample{},
		candidateSize:    baseSize,
		grow:             baseSize / 4,
	}
}

func (ps *dynamicPacketSizer) Limit() int {
	return ps.candidateSize
}

// ShouldSend checks whether the given byte count is large enough to be sent as a packet while streaming
func (ps *dynamicPacketSizer) ShouldSend(byteCount int) bool {
	return byteCount >= ps.candidateSize
}

type change int8

const (
	notChanging change = iota
	gettingFaster
	gettingSlower
)

func (ps *dynamicPacketSizer) changeInThroughput() change {
	const PValueMargin = 0.1

	t, err := mathstats.TwoSampleWelchTTest(ps.currentMetrics, ps.candidateMetrics, mathstats.LocationDiffers)
	if err != nil {
		return notChanging
	}
	if t.P < PValueMargin {
		if ps.candidateMetrics.Mean() > ps.currentMetrics.Mean() {
			return gettingFaster
		}
		return gettingSlower
	}
	return notChanging
}

func (ps *dynamicPacketSizer) reset() {
	ps.currentMetrics.Clear()
	ps.candidateMetrics.Clear()
	ps.calls = 0
	ps.settled = false
	ps.elapsed = 0
}

// Record records the total duration it took to send the given byte count while streaming
func (ps *dynamicPacketSizer) Record(byteCount int, d time.Duration) {
	const ExperimentDelay = 5 * time.Second
	const CheckFrequency = 16
	const GrowthFrequency = 32
	const InitialCandidateLen = 32
	const SettleCandidateLen = 64

	if ps.settled {
		ps.elapsed += d
		if ps.elapsed < ExperimentDelay {
			return
		}
		ps.reset()
	}

	ps.calls++
	ps.candidateMetrics.Xs = append(ps.candidateMetrics.Xs, float64(byteCount)/float64(d))
	if ps.calls%CheckFrequency == 0 {
		ps.candidateMetrics.Sorted = false
		ps.candidateMetrics.FilterOutliers()

		if len(ps.currentMetrics.Xs) == 0 {
			if len(ps.candidateMetrics.Xs) >= InitialCandidateLen {
				ps.currentMetrics, ps.candidateMetrics = ps.candidateMetrics, ps.currentMetrics
			}
			return
		}

		change := ps.changeInThroughput()
		switch change {
		case notChanging, gettingSlower:
			if len(ps.candidateMetrics.Xs) >= SettleCandidateLen {
				ps.candidateSize = ps.currentSize
				ps.settled = true
			} else {
				if change == notChanging && ps.calls%GrowthFrequency == 0 {
					ps.candidateSize += ps.grow
				}
			}

		case gettingFaster:
			ps.candidateMetrics, ps.currentMetrics = ps.currentMetrics, ps.candidateMetrics
			ps.candidateMetrics.Clear()

			ps.candidateSize += ps.grow
			ps.currentSize = ps.candidateSize
		}
	}
}
