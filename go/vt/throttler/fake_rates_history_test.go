package throttler

import "time"

// fakeRatesHistory simplifies faking the actual throttler rate for each
// 1 second interval. With the add() method up to a given time all missing
// intervals will be filled in with a given rate.
type fakeRatesHistory struct {
	*aggregatedIntervalHistory

	nextIntervalStart time.Time
	interval          time.Duration
}

func newFakeRatesHistory() *fakeRatesHistory {
	return &fakeRatesHistory{
		aggregatedIntervalHistory: newAggregatedIntervalHistory(1000, 1*time.Second, 1),
		interval:                  1 * time.Second,
	}
}

// add records the rate from the end of the last recorded interval up to
// (and including) the time "to".
// The first call to add() starts at the interval of 0s.
func (h *fakeRatesHistory) add(to time.Time, rate int64) {
	for h.nextIntervalStart.Before(to) {
		r := record{h.nextIntervalStart, rate}
		h.addPerThread(0 /* threadID */, r)
		h.nextIntervalStart = h.nextIntervalStart.Add(h.interval)
	}
}
