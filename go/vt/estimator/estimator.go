// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package estimator implements future value estimation by EWMA algorithm.
// For a given key. Estimator gives Exponential Weighted Moving Average of its
// historical values. Estimator can be used in any places where we need to
// predict the next value associated with a key.
package estimator

import (
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/cache"
	"github.com/youtube/vitess/go/ewma"
)

const (
	// DefaultCapacity is the default maximum number of records that can
	// be present in Estimator
	DefaultCapacity = 10 * 1024
)

// Estimator calculates the EWMA of all historical values associated with a key
type Estimator struct {
	mu              sync.Mutex
	records         *cache.LRUCache
	weightingFactor float64
}

// NewEstimator initializes an Estimator object with given capacity and EWMA weightingFactor
func NewEstimator(ca int64, wf float64) *Estimator {
	if ca < 1 {
		log.Infof("Invalid capacity value: %v, falling back to default(%v)", ca, DefaultCapacity)
		ca = DefaultCapacity
	}
	if wf < 0 || wf > 1 {
		log.Infof("Invalid weighting factor: %v, falling back to default(%v)", wf, ewma.DefaultWeightingFactor)
		wf = ewma.DefaultWeightingFactor
	}
	return &Estimator{
		records:         cache.NewLRUCache(ca),
		weightingFactor: wf,
	}
}

// Estimate returns the EWMA value associated with a given key
func (e *Estimator) Estimate(key string) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if v, ok := e.records.Get(key); ok {
		return v.(*ewma.EWMA).GetEWMA()
	}
	return 0
}

// AddHistory adds an additional historical value associated with a key
func (e *Estimator) AddHistory(key string, value float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if v, ok := e.records.Get(key); ok {
		v.(*ewma.EWMA).AddValue(value)
		return
	}
	v := ewma.NewEWMA(e.weightingFactor)
	v.AddValue(value)
	e.records.Set(key, v)
}
