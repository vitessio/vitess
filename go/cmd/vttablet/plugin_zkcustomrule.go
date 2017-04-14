// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the zookeeper custom rule source

import (
	_ "github.com/youtube/vitess/go/vt/vttablet/customrule/zkcustomrule"
)
