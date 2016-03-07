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
	hook       func(int64)

	// mu controls buckets & total
	mu      sync.Mutex
	buckets []int64
	total   int64
}

// NewHistogram creates a histogram with auto-generated labels
// based on the cutoffs. The buckets are categorized using the
// following criterion: cutoff[i-1] <= value < cutoff[i]. Anything
// higher than the highest cutoff is labeled as "inf".
func NewHistogram(name string, cutoffs []int64) *Histogram {
	labels := make([]string, len(cutoffs)+1)
	for i, v := range cutoffs {
		labels[i] = fmt.Sprintf("%d", v)
	}
	labels[len(labels)-1] = "inf"
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

// Add adds a new measurement to the Histogram.
func (h *Histogram) Add(value int64) {
	for i := range h.labels {
		if i == len(h.labels)-1 || value < h.cutoffs[i] {
			h.mu.Lock()
			h.buckets[i]++
			h.total += value
			h.mu.Unlock()
			break
		}
	}
	if h.hook != nil {
		h.hook(value)
	}
}

// String returns a string representation of the Histogram.
func (h *Histogram) String() string {
	b, _ := h.MarshalJSON()
	return string(b)
}

// MarshalJSON returns a JSON representation of the Histogram.
func (h *Histogram) MarshalJSON() ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	totalCount := int64(0)
	for i, label := range h.labels {
		totalCount += h.buckets[i]
		fmt.Fprintf(b, "\"%v\": %v, ", label, totalCount)
	}
	fmt.Fprintf(b, "\"%s\": %v, ", h.countLabel, totalCount)
	fmt.Fprintf(b, "\"%s\": %v", h.totalLabel, h.total)
	fmt.Fprintf(b, "}")
	return b.Bytes(), nil
}

// Counts returns a map from labels to the current count in the Histogram for that label.
func (h *Histogram) Counts() map[string]int64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	counts := make(map[string]int64, len(h.labels))
	for i, label := range h.labels {
		counts[label] = h.buckets[i]
	}
	return counts
}

// CountLabel returns the count label that was set when this Histogram was created.
func (h *Histogram) CountLabel() string {
	return h.countLabel
}

// Count returns the number of times Add has been called.
func (h *Histogram) Count() (count int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, v := range h.buckets {
		count += v
	}
	return
}

// TotalLabel returns the total label that was set when this Histogram was created.
func (h *Histogram) TotalLabel() string {
	return h.totalLabel
}

// Total returns the sum of all values that have been added to this Histogram.
func (h *Histogram) Total() (total int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.total
}

// Labels returns the labels that were set when this Histogram was created.
func (h *Histogram) Labels() []string {
	return h.labels
}

// Cutoffs returns the cutoffs that were set when this Histogram was created.
func (h *Histogram) Cutoffs() []int64 {
	return h.cutoffs
}

// Buckets returns a snapshot of the current values in all buckets.
func (h *Histogram) Buckets() []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	buckets := make([]int64, len(h.buckets))
	copy(buckets, h.buckets)
	return buckets
}
