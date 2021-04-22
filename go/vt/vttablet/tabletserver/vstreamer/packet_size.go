package vstreamer

import (
	"flag"
	"time"

	"vitess.io/vitess/go/mathstats"
	"vitess.io/vitess/go/vt/log"
)

// defaultPacketSize is the suggested packet size for VReplication streamer.
var defaultPacketSize = flag.Int("vstream_packet_size", 250000, "Suggested packet size for VReplication streamer. This is used only as a recommendation. The actual packet size may be more or less than this amount.")

// useDynamicPacketSize controls whether to use dynamic packet size adjustments to increase performance while streaming
var useDynamicPacketSize = flag.Bool("vstream_dynamic_packet_size", true, "Enable dynamic packet sizing for VReplication. This will adjust the packet size during replication to improve performance.")

// PacketSizer is a controller that adjusts the size of the packets being sent by the vstreamer at runtime
type PacketSizer interface {
	ShouldSend(byteCount int) bool
	Record(byteCount int, duration time.Duration)
}

type change int8

const (
	notChanging change = iota
	gettingFaster
	gettingSlower
)

type fixedPacketSizer struct {
	baseSize int
}

// DefaultPacketSizer creates a new PacketSizer using the default settings.
// If dynamic packet sizing is enabled, this will return a dynamicPacketSizer.
func DefaultPacketSizer() PacketSizer {
	if *useDynamicPacketSize {
		return newDynamicPacketSizer(*defaultPacketSize)
	}
	return newFixedPacketSize(*defaultPacketSize)
}

// AdjustPacketSize temporarily adjusts the default packet sizes to the given value.
// Calling the returned cleanup function resets them to their original value.
// This function is only used for testing.
func AdjustPacketSize(size int) func() {
	originalSize := *defaultPacketSize
	originalDyn := *useDynamicPacketSize

	*defaultPacketSize = size
	*useDynamicPacketSize = false

	return func() {
		*defaultPacketSize = originalSize
		*useDynamicPacketSize = originalDyn
	}
}

func newFixedPacketSize(baseSize int) PacketSizer {
	return &fixedPacketSizer{baseSize: baseSize}
}

// ShouldSend checks whether the given byte count is large enough to be sent as a packet while streaming
func (ps *fixedPacketSizer) ShouldSend(byteCount int) bool {
	return byteCount >= ps.baseSize
}

// Record records the total duration it took to send the given byte count while streaming
func (ps *fixedPacketSizer) Record(_ int, _ time.Duration) {}

type dynamicPacketSizer struct {
	last      int
	grow      int
	target    int
	hits      int
	settled   bool
	current   *mathstats.Sample
	candidate *mathstats.Sample
	elapsed   time.Duration
}

func newDynamicPacketSizer(baseSize int) PacketSizer {
	return &dynamicPacketSizer{
		last:      baseSize,
		current:   &mathstats.Sample{},
		candidate: &mathstats.Sample{},
		target:    baseSize,
		grow:      baseSize / 4,
	}
}

// ShouldSend checks whether the given byte count is large enough to be sent as a packet while streaming
func (ps *dynamicPacketSizer) ShouldSend(byteCount int) bool {
	return byteCount >= ps.target
}

func (ps *dynamicPacketSizer) changeInThroughput() change {
	const PValueMargin = 0.1

	t, err := mathstats.TwoSampleWelchTTest(ps.current, ps.candidate, mathstats.LocationDiffers)
	if err != nil {
		return notChanging
	}
	log.Infof("dynamicPacketSizer.changeInThroughput(): P = %f", t.P)
	if t.P < PValueMargin {
		if ps.candidate.Mean() > ps.current.Mean() {
			return gettingFaster
		}
		return gettingSlower
	}
	return notChanging
}

func (ps *dynamicPacketSizer) reset() {
	ps.current.Clear()
	ps.candidate.Clear()
	ps.hits = 0
	ps.settled = false
	ps.elapsed = 0
}

// Record records the total duration it took to send the given byte count while streaming
func (ps *dynamicPacketSizer) Record(byteCount int, d time.Duration) {
	const ExperimentDelay = 5 * time.Second
	const CheckFrequency = 16
	const InitialCandidateLen = 32
	const SettleCandidateLen = 64

	if ps.settled {
		ps.elapsed += d
		if ps.elapsed < ExperimentDelay {
			return
		}
		ps.reset()
	}

	ps.hits++
	ps.candidate.Xs = append(ps.candidate.Xs, float64(byteCount)/float64(d))
	if ps.hits%CheckFrequency == 0 {
		ps.candidate.Sorted = false
		ps.candidate.FilterOutliers()

		if len(ps.current.Xs) == 0 {
			if len(ps.candidate.Xs) >= InitialCandidateLen {
				ps.current, ps.candidate = ps.candidate, ps.current
				log.Infof("packetSize: stored initial sample, mean = %f", byteCount, d, ps.current.Mean())
			}
			return
		}

		change := ps.changeInThroughput()
		switch change {
		case notChanging, gettingSlower:
			if len(ps.candidate.Xs) >= SettleCandidateLen {
				ps.target = ps.last
				ps.settled = true
				log.Infof("packetSize: not changing for %d, settled at %d", len(ps.candidate.Xs), ps.target)
			} else {
				if change == notChanging {
					ps.target += ps.grow
				}
				log.Infof("packetSize: not changing for %d, grow target to %d", len(ps.candidate.Xs), ps.target)
			}

		case gettingFaster:
			ps.candidate, ps.current = ps.current, ps.candidate
			ps.candidate.Clear()

			ps.target += ps.grow
			ps.last = ps.target
			log.Infof("packetSize: faster! grow target to %d", ps.target)
		}
	}
}
