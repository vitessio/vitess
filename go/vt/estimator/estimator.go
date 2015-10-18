// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package estimator implements future value estimation by EWMA algorithm.

// For a given key. Estimator gives Exponential Weighted Moving Average of its
// values as observed from its history. Estimator can be used in any places
// where we need to estimate/predict the next value associated with a key.
package estimator

import ()
