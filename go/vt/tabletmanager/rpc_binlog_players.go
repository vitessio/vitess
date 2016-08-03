// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"golang.org/x/net/context"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// WaitBlpPosition waits until a specific filtered replication position is
// reached.
// Should be called under RPCWrapLock.
func (agent *ActionAgent) WaitBlpPosition(ctx context.Context, blpPosition *tabletmanagerdatapb.BlpPosition, waitTime time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()
	return mysqlctl.WaitBlpPosition(waitCtx, agent.MysqlDaemon, binlogplayer.QueryBlpCheckpoint(blpPosition.Uid), blpPosition.Position)
}

// StopBlp stops the binlog players, and return their positions.
// Should be called under RPCWrapLock.
func (agent *ActionAgent) StopBlp(ctx context.Context) ([]*tabletmanagerdatapb.BlpPosition, error) {
	if agent.BinlogPlayerMap == nil {
		return nil, fmt.Errorf("No BinlogPlayerMap configured")
	}
	agent.BinlogPlayerMap.Stop()
	return agent.BinlogPlayerMap.BlpPositionList()
}

// StartBlp starts the binlog players
// Should be called under RPCWrapLock.
func (agent *ActionAgent) StartBlp(ctx context.Context) error {
	if agent.BinlogPlayerMap == nil {
		return fmt.Errorf("No BinlogPlayerMap configured")
	}
	agent.BinlogPlayerMap.Start(agent.batchCtx)
	return nil
}

// RunBlpUntil runs the binlog player server until the position is reached,
// and returns the current mysql master replication position.
func (agent *ActionAgent) RunBlpUntil(ctx context.Context, bpl []*tabletmanagerdatapb.BlpPosition, waitTime time.Duration) (string, error) {
	if agent.BinlogPlayerMap == nil {
		return "", fmt.Errorf("No BinlogPlayerMap configured")
	}
	if err := agent.BinlogPlayerMap.RunUntil(ctx, bpl, waitTime); err != nil {
		return "", err
	}
	pos, err := agent.MysqlDaemon.MasterPosition()
	if err != nil {
		return "", err
	}
	return replication.EncodePosition(pos), nil
}
