// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"github.com/youtube/vitess/go/mysql/proto"
)

// This file contains the actions that exist as RPC only on the ActionAgent.
// The various rpc server implementations just call these.
// (if an action can be both an ActionNode and a RPC, it's implemented
// in the actor code).

// ExecuteFetch will execute the given query, possibly disabling binlogs.
func (agent *ActionAgent) ExecuteFetch(query string, maxrows int, wantFields, disableBinlogs bool) (*proto.QueryResult, error) {
	// get a connection
	conn, err := agent.Mysqld.GetDbaConnection()
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	// disable binlogs if necessary
	if disableBinlogs {
		_, err := conn.ExecuteFetch("SET sql_log_bin = OFF", 0, false)
		if err != nil {
			return nil, err
		}
	}

	// run the query
	qr, err := conn.ExecuteFetch(query, maxrows, wantFields)

	// re-enable binlogs if necessary
	if disableBinlogs && !conn.IsClosed() {
		conn.ExecuteFetch("SET sql_log_bin = ON", 0, false)
		if err != nil {
			// if we can't reset the sql_log_bin flag,
			// let's just close the connection.
			conn.Close()
		}
	}

	return qr, err
}
