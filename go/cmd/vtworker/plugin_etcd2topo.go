// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// This plugin imports etcd2topo to register the etcd2 implementation of TopoServer.

import (
	_ "github.com/youtube/vitess/go/vt/topo/etcd2topo"
)
