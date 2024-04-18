/*
Copyright 2019 The Vitess Authors.

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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemory(t *testing.T) {
	m := newMemory(5, 1*time.Second, 0.10)

	// Add several good rates.
	err := m.markGood(201)
	require.NoError(t, err)

	want200 := int64(200)
	got := m.highestGood()
	require.Equal(t, want200, got, "memory with one good entry")

	err = m.markGood(101)
	require.NoError(t, err)

	got = m.highestGood()
	require.Equal(t, want200, got, "wrong order within memory")

	err = m.markGood(301)
	require.NoError(t, err)

	want300 := int64(300)
	got = m.highestGood()
	require.Equal(t, want300, got, "wrong order within memory")

	err = m.markGood(306)
	require.NoError(t, err)

	want305 := int64(305)
	got = m.highestGood()
	require.Equal(t, want305, got, "wrong order within memory")

	// 300 and 305 will turn from good to bad.
	got = m.lowestBad()
	require.Equal(t, int64(0), got, "lowestBad should return zero value when no bad rate is recorded yet")

	err = m.markBad(300, sinceZero(0))
	require.NoError(t, err)

	got = m.lowestBad()
	require.Equal(t, want300, got, "bad rate was not recorded")

	got = m.highestGood()
	require.Equal(t, want200, got, "new lower bad rate did not invalidate previous good rates")

	err = m.markBad(311, sinceZero(0))
	require.NoError(t, err)

	got = m.lowestBad()
	require.Equal(t, want300, got, "bad rates higher than the current one should be ignored")

	// a good 601 will be ignored because the first bad is at 300.
	err = m.markGood(601)
	require.Error(t, err, "good rates cannot go beyond the lowest bad rate")

	got = m.lowestBad()
	require.Equal(t, want300, got, "good rates cannot go beyond the lowest bad rate")

	got = m.highestGood()
	require.Equal(t, want200, got, "good rates beyond the lowest bad rate must be ignored")

	// 199 will be rounded up to 200.
	err = m.markBad(199, sinceZero(0))
	require.NoError(t, err)

	got = m.lowestBad()
	require.Equal(t, want200, got, "bad rate was not updated")

	want100 := int64(100)
	got = m.highestGood()
	require.Equal(t, want100, got, "previous highest good rate was not marked as bad")
}

func TestMemory_markDownIgnoresDrasticBadValues(t *testing.T) {
	m := newMemory(1, 1*time.Second, 0.10)
	good := int64(1000)
	bad := int64(1001)

	err := m.markGood(good)
	require.NoError(t, err)

	err = m.markBad(bad, sinceZero(0))
	require.NoError(t, err)

	got := m.highestGood()
	require.Equal(t, good, got, "good rate was not correctly inserted")

	got = m.lowestBad()
	require.Equal(t, bad, got, "bad rate was not correctly inserted")

	err = m.markBad(500, sinceZero(0))
	require.Error(t, err, "bad rate should have been ignored and an error should have been returned")

	got = m.highestGood()
	require.Equal(t, good, got, "bad rate should have been ignored")

	got = m.lowestBad()
	require.Equal(t, bad, got, "bad rate should have been ignored")
}

func TestMemory_Aging(t *testing.T) {
	m := newMemory(1, 2*time.Second, 0.10)

	err := m.markBad(100, sinceZero(0))
	require.NoError(t, err)

	got := m.lowestBad()
	require.Equal(t, int64(100), got, "bad rate was not correctly inserted")

	// Bad rate successfully ages by 10%.
	m.ageBadRate(sinceZero(2 * time.Second))

	got = m.lowestBad()
	require.Equal(t, int64(110), got, "bad rate should have been increased due to its age")

	// A recent aging resets the age timer.
	m.ageBadRate(sinceZero(2 * time.Second))
	got = m.lowestBad()
	require.Equal(t, int64(110), got, "a bad rate should not age again until the age is up again")

	// The age timer will be reset if the bad rate changes.
	err = m.markBad(100, sinceZero(3*time.Second))
	require.NoError(t, err)

	m.ageBadRate(sinceZero(4 * time.Second))

	got = m.lowestBad()
	require.Equal(t, int64(100), got, "bad rate must not age yet")

	// The age timer won't be reset when the rate stays the same.
	err = m.markBad(100, sinceZero(4*time.Second))
	require.NoError(t, err)

	m.ageBadRate(sinceZero(5 * time.Second))

	got = m.lowestBad()
	require.Equal(t, int64(110), got, "bad rate should have aged again")

	// Update the aging config. It will be effective immediately.
	m.updateAgingConfiguration(1*time.Second, 0.05)
	m.ageBadRate(sinceZero(6 * time.Second))

	got = m.lowestBad()
	require.Equal(t, int64(115), got, "bad rate should have aged after the configuration update")

	// If the new bad rate is not higher, it should increase by the memory granularity at least.
	m.markBad(5, sinceZero(10*time.Second))
	m.ageBadRate(sinceZero(11 * time.Second))

	got = m.lowestBad()
	require.Equal(t, int64(5+memoryGranularity), got, "bad rate should have aged after the configuration update")
}
