/*
Copyright 2017 Google Inc.

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

package throttler

import (
	"fmt"
	"sort"
	"time"
)

// memory tracks "good" and "bad" throttler rates where good rates are below
// the system capacity and bad are above it.
//
// It is based on the fact that the MySQL performance degrades with an
// increasing number of rows. Therefore, the implementation stores only one
// bad rate which will get lower over time and turn known good rates into
// bad ones.
//
// To protect against temporary performance degradations, a stable bad rate,
// which hasn't changed for a certain time, "ages out" and will be increased
// again. This ensures that rates above past bad rates will be tested again in
// the future.
//
// To simplify tracking all possible rates, they are slotted into buckets
// e.g. of the size 5.
type memory struct {
	bucketSize int

	good             []int64
	bad              int64
	nextBadRateAging time.Time

	ageBadRateAfter time.Duration
	badRateIncrease float64
}

func newMemory(bucketSize int, ageBadRateAfter time.Duration, badRateIncrease float64) *memory {
	if bucketSize == 0 {
		bucketSize = 1
	}
	return &memory{
		bucketSize:      bucketSize,
		good:            make([]int64, 0),
		ageBadRateAfter: ageBadRateAfter,
		badRateIncrease: badRateIncrease,
	}
}

func (m *memory) updateAgingConfiguration(ageBadRateAfter time.Duration, badRateIncrease float64) {
	if !m.nextBadRateAging.IsZero() {
		// Adjust the current age timer immediately.
		m.nextBadRateAging = m.nextBadRateAging.Add(ageBadRateAfter).Add(-m.ageBadRateAfter)
	}
	m.ageBadRateAfter = ageBadRateAfter
	m.badRateIncrease = badRateIncrease
}

// int64Slice is used to sort int64 slices.
type int64Slice []int64

func (a int64Slice) Len() int           { return len(a) }
func (a int64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64Slice) Less(i, j int) bool { return a[i] < a[j] }

func searchInt64s(a []int64, x int64) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

func (m *memory) markGood(rate int64) error {
	rate = m.roundDown(rate)

	if lowestBad := m.lowestBad(); lowestBad != 0 && rate > lowestBad {
		return fmt.Errorf("ignoring higher good rate of %v because we assume that the known maximum capacity (currently at %v) can only degrade", rate, lowestBad)
	}

	// Skip rates which already exist.
	i := searchInt64s(m.good, rate)
	if i < len(m.good) && m.good[i] == rate {
		return nil
	}

	m.good = append(m.good, rate)
	sort.Sort(int64Slice(m.good))
	return nil
}

func (m *memory) markBad(rate int64, now time.Time) error {
	// Bad rates are rounded up instead of down to not be too extreme on the
	// reduction and account for some margin of error.
	rate = m.roundUp(rate)

	// Ignore higher bad rates than the current one.
	if m.bad != 0 && rate >= m.bad {
		return nil
	}

	// Ignore bad rates which are too drastic. This prevents that temporary
	// hiccups e.g. during a reparent, are stored in the memory.
	// TODO(mberlin): Remove this once we let bad values expire over time.
	highestGood := m.highestGood()
	if rate < highestGood {
		decrease := float64(highestGood) - float64(rate)
		degradation := decrease / float64(highestGood)
		if degradation > 0.1 {
			return fmt.Errorf("ignoring lower bad rate of %v because such a high degradation (%.1f%%) is unlikely (current highest good: %v)", rate, degradation*100, highestGood)
		}
	}

	// Delete all good values which turned bad.
	goodLength := len(m.good)
	for i := goodLength - 1; i >= 0; i-- {
		goodRate := m.good[i]
		if goodRate >= rate {
			goodLength = i
		} else {
			break
		}
	}
	m.good = m.good[:goodLength]

	m.bad = rate
	m.touchBadRateAge(now)
	return nil
}

// touchBadRateAge records that the bad rate was changed and the aging should be
// further delayed.
func (m *memory) touchBadRateAge(now time.Time) {
	m.nextBadRateAging = now.Add(m.ageBadRateAfter)
}

func (m *memory) ageBadRate(now time.Time) {
	if m.badRateIncrease == 0 {
		return
	}
	if m.bad == 0 {
		return
	}
	if m.nextBadRateAging.IsZero() {
		return
	}
	if now.Before(m.nextBadRateAging) {
		return
	}

	newBad := float64(m.bad) * (1 + m.badRateIncrease)
	if int64(newBad) == m.bad {
		// Increase had no effect. Increase it at least by the granularity.
		newBad += memoryGranularity
	}
	m.bad = int64(newBad)
	m.touchBadRateAge(now)
}

func (m *memory) highestGood() int64 {
	if len(m.good) == 0 {
		return 0
	}

	return m.good[len(m.good)-1]
}

func (m *memory) lowestBad() int64 {
	return m.bad
}

func (m *memory) roundDown(rate int64) int64 {
	return rate / int64(m.bucketSize) * int64(m.bucketSize)
}

func (m *memory) roundUp(rate int64) int64 {
	ceil := rate / int64(m.bucketSize) * int64(m.bucketSize)
	if rate%int64(m.bucketSize) != 0 {
		ceil += int64(m.bucketSize)
	}
	return ceil
}
