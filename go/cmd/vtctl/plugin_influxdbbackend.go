// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// This plugin imports influxdbbackend to register the influxdbbackend stats backend.

import (
	_ "github.com/youtube/vitess/go/stats/influxdbbackend"
)
