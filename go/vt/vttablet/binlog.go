// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttablet

// This file handles the binlog players launched on masters for filtered
// replication

import (
	"time"

	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo"
)

// BinlogPlayerController controls one player
type BinlogPlayerController struct {
	ts       topo.Server
	vtClient mysqlctl.VtClient
	keyspace string
	keyRange key.KeyRange

	// Player is the BinlogPlayer when we have one
	player *mysqlctl.BinlogPlayer

	// interrupted is the channel to close to stop the playback
	interrupted chan struct{}

	// TODO(alainjobart): add state
	// TODO(alainjobart): add statsJson, include stats and player stats if any
	// TODO(alainjobart): figure out if we need a lock on structure (for stats)
}

func NewBinlogController(ts topo.Server, vtClient mysqlctl.VtClient, keyspace string, keyRange key.KeyRange) *BinlogPlayerController {
	return &BinlogPlayerController{
		ts:          ts,
		vtClient:    vtClient,
		keyspace:    keyspace,
		keyRange:    keyRange,
		interrupted: make(chan struct{}, 1),
	}
}

func (bpc *BinlogPlayerController) Start() {
	go bpc.Loop()
}

func (bpc *BinlogPlayerController) Stop() {
	close(bpc.interrupted)
}

func (bpc *BinlogPlayerController) Loop() {
	for {
		// Read the start position
		startPosition, err := mysqlctl.ReadStartPosition(bpc.vtClient, string(bpc.keyRange.Start.Hex()), string(bpc.keyRange.End.Hex()))
		if err != nil {
			relog.Warning("BinlogPlayerController: can't read startPosition: %v", err)
			time.Sleep(5)
			continue
		}

		// Find the server list

		// Pick a server (same if it's available, if not clear master file / pos and keep only group id)

		// create the player
		bpc.player, err = mysqlctl.NewBinlogPlayer(bpc.vtClient, startPosition /*tables*/, nil /*txnBatch*/, 1 /*maxTxnInterval*/, 30*time.Second /*execDdl*/, false)
		if err != nil {
			relog.Warning("BinlogPlayerController: can't create player: %v", err)
			time.Sleep(5)
			continue
		}

		// run player loop until it's done
		err = bpc.player.ApplyBinlogEvents(bpc.interrupted)
		if err != nil {
			relog.Error("Error in applying binlog events, err %v", err)
			time.Sleep(5)
		} else {
			// we were interrupted
			break
		}
	}
	relog.Info("Exited main binlog player loop for %v", bpc.keyRange)
}

// BinlogPlayerMap controls all the players
type BinlogPlayerMap struct {
	ts       topo.Server
	vtClient mysqlctl.VtClient
	players  map[key.KeyRange]*BinlogPlayerController
}

func NewBinlogPlayerMap(ts topo.Server, vtClient mysqlctl.VtClient) *BinlogPlayerMap {
	return &BinlogPlayerMap{
		ts:       ts,
		vtClient: vtClient,
		players:  make(map[key.KeyRange]*BinlogPlayerController),
	}
}

// TODO(alainjobart) add stats, register them

func (blm *BinlogPlayerMap) AddPlayer(keyspace string, keyRange key.KeyRange) {
	bpc, ok := blm.players[keyRange]
	if ok {
		relog.Info("Already playing logs for %v", keyRange)
		return
	}
	relog.Info("Starting playing logs from %v", keyRange)
	bpc = NewBinlogController(blm.ts, blm.vtClient, keyspace, keyRange)
	blm.players[keyRange] = bpc
}

func (blm *BinlogPlayerMap) StopAllPlayers() {
	for keyRange, bpc := range blm.players {
		relog.Info("Stopping binlog player for %v", keyRange)
		bpc.Stop()
	}
	blm.players = make(map[key.KeyRange]*BinlogPlayerController)
}
