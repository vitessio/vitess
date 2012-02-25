/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package stats

import (
	"bytes"
	"expvar"
	"fmt"
	"sync/atomic"
)

type Histogram struct {
	cutoffs    []int64
	labels     []string
	countLabel string
	totalLabel string
	buckets    []int64
	total      int64
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
		cutoffs,
		labels,
		countLabel,
		totalLabel,
		make([]int64, len(labels)),
		0,
	}
	if name != "" {
		expvar.Publish(name, h)
	}
	return h
}

func (h *Histogram) Add(value int64) {
	for i := range h.labels {
		if i == len(h.labels)-1 || value <= h.cutoffs[i] {
			atomic.AddInt64(&h.buckets[i], 1)
			atomic.AddInt64(&h.total, value)
			break
		}
	}
}

func (h *Histogram) String() string {
	b, _ := h.MarshalJSON()
	return string(b)
}

func (h *Histogram) MarshalJSON() ([]byte, error) {
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
	for _, v := range h.buckets {
		count += v
	}
	return count
}
