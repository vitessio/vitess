// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"bytes"
	"expvar"
	"fmt"
	"sync"
)

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

func NewHistogram(name string, cutoffs []int64) *Histogram {
	labels := make([]string, len(cutoffs)+1)
	for i, v := range cutoffs {
		labels[i] = fmt.Sprintf("%d", v)
	}
	labels[len(labels)-1] = "Max"
	return NewGenericHistogram(name, cutoffs, labels, "Count", "Total")
}

func NewGenericHistogram(name string, cutoffs []int64, labels []string, countLabel, totalLabel string) *Histogram {
	h := &Histogram{
		cutoffs:    cutoffs,
		labels:     labels,
		countLabel: countLabel,
		totalLabel: totalLabel,
		buckets:    make([]int64, len(labels)),
		total:      0,
	}
	if name != "" {
		expvar.Publish(name, h)
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

func (h *Histogram) Count() (count int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, v := range h.buckets {
		count += v
	}
	return count
}
