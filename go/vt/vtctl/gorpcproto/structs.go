// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package gorpcproto contains the Go RPC definitions of the structures used to
execute remote vtctl commands.
*/
package gorpcproto

import (
	"time"
)

// ExecuteVtctlCommandArgs contains the parameters for the ExecuteVtctlCommand
// RPC call.
type ExecuteVtctlCommandArgs struct {
	Args []string

	// ActionTimeout can be defaulted to wrangler.DefaultActionTimeout
	// for a decent default value.
	ActionTimeout time.Duration
}
