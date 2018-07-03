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
	"errors"
	"time"

	"golang.org/x/net/context"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// WaitBlpPosition waits until a specific filtered replication position is
// reached.
func (agent *ActionAgent) WaitBlpPosition(ctx context.Context, blpPosition *tabletmanagerdatapb.BlpPosition, waitTime time.Duration) error {
	return errors.New("WaitBlpPosition deprecated")
}

// StopBlp stops the binlog players, and return their positions.
func (agent *ActionAgent) StopBlp(ctx context.Context) ([]*tabletmanagerdatapb.BlpPosition, error) {
	return nil, errors.New("StopBlp deprecated")
}

// StartBlp starts the binlog players
func (agent *ActionAgent) StartBlp(ctx context.Context) error {
	return errors.New("StartBlp deprecated")
}

// RunBlpUntil runs the binlog player server until the position is reached,
// and returns the current mysql master replication position.
func (agent *ActionAgent) RunBlpUntil(ctx context.Context, bpl []*tabletmanagerdatapb.BlpPosition, waitTime time.Duration) (string, error) {
	return "", errors.New("RunBlpUntil deprecated")
}
