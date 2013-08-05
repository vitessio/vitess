// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttablet

// This file handles the binlog players launched on masters for filtered
// replication

import (
	"time"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo"
)

// BinlogPlayerController controls one player
type BinlogPlayerController struct {
	ts       topo.Server
	vtClient mysqlctl.VtClient
	keyspace string
	source   topo.SourceShard

	// Player is the BinlogPlayer when we have one
	player *mysqlctl.BinlogPlayer

	// interrupted is the channel to close to stop the playback
	interrupted chan struct{}

	// TODO(alainjobart): add state
	// TODO(alainjobart): add statsJson, include stats and player stats if any
	// TODO(alainjobart): figure out if we need a lock on structure (for stats)
}

func NewBinlogController(ts topo.Server, vtClient mysqlctl.VtClient, keyspace string, source topo.SourceShard) *BinlogPlayerController {
	return &BinlogPlayerController{
		ts:          ts,
		vtClient:    vtClient,
		keyspace:    keyspace,
		source:      source,
		interrupted: make(chan struct{}, 1),
	}
}

func (bpc *BinlogPlayerController) Start() {
	relog.Info("Starting binlog player for %v", bpc.source)
	go bpc.Loop()
}

func (bpc *BinlogPlayerController) Stop() {
	relog.Info("Stopping binlog player for %v", bpc.source)
	close(bpc.interrupted)
}

func (bpc *BinlogPlayerController) Loop() {
	for {
		// Read the start position
		startPosition, err := mysqlctl.ReadStartPosition(bpc.vtClient, string(bpc.source.KeyRange.Start.Hex()), string(bpc.source.KeyRange.End.Hex()))
		if err != nil {
			relog.Warning("BinlogPlayerController: can't read startPosition: %v", err)
			time.Sleep(5)
			continue
		}

		// TODO(alainjobart): Find the server list

		// TODO(alainjobart): Pick a server (same if it's available,
		// if not clear master file / pos and keep only group id)

		// Create the player.
		bpc.player, err = mysqlctl.NewBinlogPlayer(bpc.vtClient, startPosition, nil /*tables*/, 1 /*txnBatch*/, 30*time.Second /*maxTxnInterval*/, false /*execDdl*/)
		if err != nil {
			relog.Warning("BinlogPlayerController: can't create player: %v", err)
			time.Sleep(5)
			continue
		}

		// Run player loop until it's done.
		err = bpc.player.ApplyBinlogEvents(bpc.interrupted)
		if err != nil {
			relog.Error("Error in applying binlog events, err %v", err)
			time.Sleep(5)
		} else {
			// We were interrupted.
			break
		}
	}
	relog.Info("Exited main binlog player loop for %v", bpc.source)
}

// BinlogPlayerMap controls all the players
type BinlogPlayerMap struct {
	ts       topo.Server
	dbConfig *mysql.ConnectionParams
	players  map[topo.SourceShard]*BinlogPlayerController
}

func NewBinlogPlayerMap(ts topo.Server, dbConfig *mysql.ConnectionParams) *BinlogPlayerMap {
	return &BinlogPlayerMap{
		ts:       ts,
		dbConfig: dbConfig,
		players:  make(map[topo.SourceShard]*BinlogPlayerController),
	}
}

// TODO(alainjobart) add stats, register them

func (blm *BinlogPlayerMap) AddPlayer(keyspace string, source topo.SourceShard) {
	bpc, ok := blm.players[source]
	if ok {
		relog.Info("Already playing logs for %v", source)
		return
	}

	// create the db connection, connect it
	vtClient := mysqlctl.NewDbClient(blm.dbConfig)
	if err := vtClient.Connect(); err != nil {
		relog.Error("BinlogPlayerMap: can't connect to database: %v", err)
		return
	}

	bpc = NewBinlogController(blm.ts, vtClient, keyspace, source)
	blm.players[source] = bpc
	bpc.Start()
}

func (blm *BinlogPlayerMap) StopAllPlayers() {
	for _, bpc := range blm.players {
		bpc.Stop()
	}
	blm.players = make(map[topo.SourceShard]*BinlogPlayerController)
}

// RefreshMap reads the right data from topo.Server and makes sure
// we're playing the right logs
func (blm *BinlogPlayerMap) RefreshMap(tablet topo.Tablet) {
	relog.Info("Refreshing map of binlog players")

	// read the shard to get SourceShards
	shardInfo, err := blm.ts.GetShard(tablet.Keyspace, tablet.Shard)
	if err != nil {
		relog.Error("Cannot read shard for this tablet: %v", tablet.Alias())
		return
	}

	// get the existing sources and build a map of sources to remove
	toRemove := make(map[topo.SourceShard]bool)
	for source, _ := range blm.players {
		toRemove[source] = true
	}

	// for each source, add it if not there, and delete from toRemove
	for _, source := range shardInfo.SourceShards {
		blm.AddPlayer(tablet.Keyspace, source)
		delete(toRemove, source)
	}

	// remove all entries from toRemove
	for source, _ := range toRemove {
		blm.players[source].Stop()
		delete(blm.players, source)
	}
}
