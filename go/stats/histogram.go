// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"bytes"
	"fmt"
	"sync"
)

// Histogram tracks counts and totals while
// splitting the counts under different buckets
// using specified cutoffs.
type Histogram struct {
	cutoffs    []int64
	labels     []string
	countLabel string
	totalLabel string

	// mu controls buckets & total
	mu      sync.Mutex
	buckets []int64
	total   int64
}

// NewHistogram creates a histogram with auto-generated labels
// based on the cutoffs. The buckets are categorized using the
// following criterion: cutoff[i-1] < value <= cutoff[i]. Anything
// higher than the highest cutoff is labeled as "Max".
func NewHistogram(name string, cutoffs []int64) *Histogram {
	labels := make([]string, len(cutoffs)+1)
	for i, v := range cutoffs {
		labels[i] = fmt.Sprintf("%d", v)
	}
	labels[len(labels)-1] = "Max"
	return NewGenericHistogram(name, cutoffs, labels, "Count", "Total")
}

// NewGenericHistogram creates a histogram where all the labels are
// supplied by the caller. The number of labels has to be one more than
// the number of cutoffs because the last label captures everything that
// exceeds the highest cutoff.
func NewGenericHistogram(name string, cutoffs []int64, labels []string, countLabel, totalLabel string) *Histogram {
	if len(cutoffs) != len(labels)-1 {
		panic("mismatched cutoff and label lengths")
	}
	h := &Histogram{
		cutoffs:    cutoffs,
		labels:     labels,
		countLabel: countLabel,
		totalLabel: totalLabel,
		buckets:    make([]int64, len(labels)),
		total:      0,
	}
	if name != "" {
		Publish(name, h)
	}
	return h
}

func (h *Histogram) Add(value int64) {
	for i := range h.labels {
		if i == len(h.labels)-1 || value <= h.cutoffs[i] {
			h.mu.Lock()
			h.buckets[i] += 1
			h.total += value
			h.mu.Unlock()
			return
		}
	}
}

func (h *Histogram) String() string {
	b, _ := h.MarshalJSON()
	return string(b)
}

func (h *Histogram) MarshalJSON() ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	totalCount := int64(0)
	for i, label := range h.labels {
		fmt.Fprintf(b, "\"%v\": %v, ", label, h.buckets[i])
		totalCount += h.buckets[i]
	}
	fmt.Fprintf(b, "\"%s\": %v, ", h.countLabel, totalCount)
	fmt.Fprintf(b, "\"%s\": %v", h.totalLabel, h.total)
	fmt.Fprintf(b, "}")
	return b.Bytes(), nil
}

func (h *Histogram) Counts() map[string]int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	counts := make(map[string]int64, len(h.labels))
	for i, label := range h.labels {
		counts[label] = h.buckets[i]
	}
	return counts
}

func (h *Histogram) Count() (count int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, v := range h.buckets {
		count += v
	}
	return
}

func (h *Histogram) Total() (total int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.total
}
