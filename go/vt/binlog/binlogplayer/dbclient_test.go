/*
Copyright 2026 The Vitess Authors.

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

package binlogplayer

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

// TestSetMultiStatementsOnAClosedConnection covers what a dead connection
// remembers. Closing it leaves the capability it negotiated on record, so a
// client that answered from that would tell its caller the capability was
// granted on a connection that cannot carry it, and the caller would go on to
// build a batch for it.
func TestSetMultiStatementsOnAClosedConnection(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { server.Close() })

	conn := mysql.NewConnForTest(client)
	conn.Capabilities = mysql.CapabilityClientMultiStatements
	conn.Close()

	dc := &dbClientImpl{dbConn: conn}
	require.Error(t, dc.SetMultiStatements(true), "a closed connection cannot grant the capability, whatever it last negotiated")
}
