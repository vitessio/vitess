/*
Copyright 2017 Google Inc.

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

package tabletmanager

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/mysqlctl"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// WaitBlpPosition waits until a specific filtered replication position is
// reached.
func (agent *ActionAgent) WaitBlpPosition(ctx context.Context, blpPosition *tabletmanagerdatapb.BlpPosition, waitTime time.Duration) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	waitCtx, cancel := context.WithTimeout(ctx, waitTime)
	defer cancel()
	return mysqlctl.WaitBlpPosition(waitCtx, agent.MysqlDaemon, binlogplayer.QueryBlpCheckpoint(blpPosition.Uid), blpPosition.Position)
}

// StopBlp stops the binlog players, and return their positions.
func (agent *ActionAgent) StopBlp(ctx context.Context) ([]*tabletmanagerdatapb.BlpPosition, error) {
	if err := agent.lock(ctx); err != nil {
		return nil, err
	}
	defer agent.unlock()

	if agent.BinlogPlayerMap == nil {
		return nil, fmt.Errorf("No BinlogPlayerMap configured")
	}
	agent.BinlogPlayerMap.Stop()
	return agent.BinlogPlayerMap.BlpPositionList()
}

// StartBlp starts the binlog players
func (agent *ActionAgent) StartBlp(ctx context.Context) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

	if agent.BinlogPlayerMap == nil {
		return fmt.Errorf("No BinlogPlayerMap configured")
	}
	agent.BinlogPlayerMap.Start(agent.batchCtx)
	return nil
}

// RunBlpUntil runs the binlog player server until the position is reached,
// and returns the current mysql master replication position.
func (agent *ActionAgent) RunBlpUntil(ctx context.Context, bpl []*tabletmanagerdatapb.BlpPosition, waitTime time.Duration) (string, error) {
	if err := agent.lock(ctx); err != nil {
		return "", err
	}
	defer agent.unlock()

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
	return mysql.EncodePosition(pos), nil
}
