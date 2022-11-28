/*
Copyright 2022 The Vitess Authors.

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

// Package backoff implements different backoff strategies for retrying failed
// operations in VTAdmin.
//
// It is first a reimplementation of grpc-go's internal exponential backoff
// strategy, with one modification to prevent a jittered backoff from exceeding
// the MaxDelay specified in a config.
//
// Then, for other use-cases, it implements a linear backoff strategy, as well
// as a "none" strategy which is primarly intended for use in tests.
package backoff

import (
	"fmt"
	"strings"
	"time"

	grpcbackoff "google.golang.org/grpc/backoff"

	"vitess.io/vitess/go/vt/log"
	vtrand "vitess.io/vitess/go/vt/vtadmin/internal/rand"
)

// Strategy defines the interface for different backoff strategies.
type Strategy interface {
	// Backoff returns the amount of time to backoff for the given retry count.
	Backoff(retries int) time.Duration
}

var (
	DefaultExponential = Exponential{grpcbackoff.DefaultConfig}
	DefaultLinear      = Linear{grpcbackoff.DefaultConfig}
	DefaultNone        = None{}
)

// Exponential implements an exponential backoff strategy with optional jitter.
type Exponential struct {
	Config grpcbackoff.Config
}

// Backoff is part of the Strategy interface.
func (e Exponential) Backoff(retries int) time.Duration {
	return backoffCommon(retries, e.Config, func(cur float64) float64 { return cur * e.Config.Multiplier })
}

// Linear implements a linear backoff strategy with optional jitter.
type Linear struct {
	Config grpcbackoff.Config
}

// Backoff is part of the Strategy interface.
func (l Linear) Backoff(retries int) time.Duration {
	return backoffCommon(retries, l.Config, func(cur float64) float64 { return cur + l.Config.Multiplier })
}

func backoffCommon(retries int, cfg grpcbackoff.Config, adjust func(cur float64) float64) time.Duration {
	if retries == 0 {
		return cfg.BaseDelay
	}

	backoff, max := float64(cfg.BaseDelay), float64(cfg.MaxDelay)
	for backoff < max && retries > 0 {
		backoff = adjust(backoff)
		retries--
	}
	if backoff > max {
		backoff = max
	}
	// Randomize backoff delays so that if a cluster of requests start at
	// the same time, they won't operate in lockstep.
	backoff *= 1 + cfg.Jitter*(vtrand.Float64()*2-1)
	if backoff < 0 {
		return 0
	}

	// NOTE: We differ from grpc's exponential backoff here, which actually can
	// jitter to a backoff that exceeds the config's MaxDelay, which in my (ajm188)
	// opinion is a bug.
	if backoff > max {
		backoff = max
	}

	return time.Duration(backoff)
}

// None implements a "backoff" strategy that can be summarized as "don't".
type None struct{}

// Backoff is part of the Strategy interface.
func (None) Backoff(int) time.Duration { return 0 }

// Get returns a backoff Strategy for the specified strategy name, with the
// given config. Strategy lookup is case-insensitive, and the empty string
// defaults to an exponential strategy. It panics if an unsupported strategy
// name is specified.
//
// Currently-supported strategies are "exponential", "linear", and "none".
func Get(strategy string, cfg grpcbackoff.Config) Strategy {
	switch strings.ToLower(strategy) {
	case "":
		log.Warningf("no backoff strategy specified; defaulting to exponential")
		fallthrough
	case "exponential":
		return Exponential{cfg}
	case "linear":
		return Linear{cfg}
	case "none":
		return None{}
	default:
		panic(fmt.Sprintf("unknown backoff strategy: %s", strategy))
	}
}
