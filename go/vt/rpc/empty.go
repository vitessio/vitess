// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpc

// RPC structs shared between many components

type UnusedRequest string

var NilRequest = new(UnusedRequest)

type UnusedResponse string

var NilResponse = new(UnusedResponse)
