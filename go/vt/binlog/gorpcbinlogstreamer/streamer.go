// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcbinlogstreamer

import (
	"github.com/youtube/vitess/go/rpcwrap"
	"github.com/youtube/vitess/go/vt/binlog"
)

type UpdateStream struct {
	*binlog.UpdateStream
}

var server *UpdateStream

func init() {
	binlog.RegisterUpdateStreamServices = append(binlog.RegisterUpdateStreamServices, func(updateStream *binlog.UpdateStream) {
		server = &UpdateStream{updateStream}
		rpcwrap.RegisterAuthenticated(server)
	})
}
