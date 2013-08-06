// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttablet

// This file handles the binlog players launched on masters for filtered
// replication

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo"
)

const (
	BINLOG_PLAYER_CONNECTING = iota
	BINLOG_PLAYER_PLAYING
	BINLOG_PLAYER_SLEEPING
)

// BinlogPlayerController controls one player
type BinlogPlayerController struct {
	ts       topo.Server
	dbConfig *mysql.ConnectionParams
	keyspace string
	source   topo.SourceShard

	// Player is the BinlogPlayer when we have one, protected by mu
	player *mysqlctl.BinlogPlayer
	mu     sync.Mutex

	// states is our state accounting variable, for stats
	states *stats.States

	// interrupted is the channel to close to stop the playback
	interrupted chan struct{}
}

func NewBinlogController(ts topo.Server, dbConfig *mysql.ConnectionParams, keyspace string, source topo.SourceShard) *BinlogPlayerController {
	blc := &BinlogPlayerController{
		ts:          ts,
		dbConfig:    dbConfig,
		keyspace:    keyspace,
		source:      source,
		interrupted: make(chan struct{}, 1),
	}
	blc.states = stats.NewStates("", []string{
		"Connecting",
		"Playing",
		"Sleeping",
	}, time.Now(), BINLOG_PLAYER_CONNECTING)
	return blc
}

func (blc *BinlogPlayerController) statsJSON() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "{")
	blc.mu.Lock()
	if blc.player != nil {
		fmt.Fprintf(buf, "\n \"Player\": %v", blc.player.StatsJSON())
	}
	blc.mu.Unlock()
	fmt.Fprintf(buf, "\n \"States\": %v", blc.states.String())
	fmt.Fprintf(buf, "\n}")
	return buf.String()
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
		err := bpc.Iteration()
		if err == nil {
			// this happens when we get interrupted
			break
		}
		relog.Warning("BinlogPlayerController(%v-%v): %v", bpc.source.KeyRange.Start, bpc.source.KeyRange.End, err)

		// sleep for a bit before retrying to connect
		bpc.states.SetState(BINLOG_PLAYER_SLEEPING)
		time.Sleep(5)
	}

	relog.Info("Exited main binlog player loop for %v", bpc.source)
}

func (bpc *BinlogPlayerController) Iteration() (err error) {
	defer func() {
		if x := recover(); x != nil {
			relog.Error("BinlogPlayerController caught panic: %v", x)
			err = fmt.Errorf("panic: %v", x)
		}
	}()
	bpc.states.SetState(BINLOG_PLAYER_CONNECTING)

	// create the db connection, connect it
	vtClient := mysqlctl.NewDbClient(bpc.dbConfig)
	if err := vtClient.Connect(); err != nil {
		return fmt.Errorf("can't connect to database: %v", err)
	}
	defer vtClient.Close()

	// Read the start position
	startPosition, err := mysqlctl.ReadStartPosition(vtClient, string(bpc.source.KeyRange.Start.Hex()), string(bpc.source.KeyRange.End.Hex()))
	if err != nil {
		return fmt.Errorf("can't read startPosition: %v", err)
	}

	// TODO(alainjobart): Find the server list

	// TODO(alainjobart): Pick a server (same if it's available,
	// if not clear master file / pos and keep only group id)

	// Create the player.
	bpc.mu.Lock()
	bpc.player, err = mysqlctl.NewBinlogPlayer(vtClient, startPosition, nil /*tables*/, 1 /*txnBatch*/, 30*time.Second /*maxTxnInterval*/, false /*execDdl*/)
	bpc.mu.Unlock()
	if err != nil {
		return fmt.Errorf("can't create player: %v", err)
	}

	// Run player loop until it's done.
	bpc.states.SetState(BINLOG_PLAYER_PLAYING)
	err = bpc.player.ApplyBinlogEvents(bpc.interrupted)

	bpc.mu.Lock()
	bpc.player = nil
	bpc.mu.Unlock()

	return err
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

func RegisterBinlogPlayerMap(blm *BinlogPlayerMap) {
	stats.PublishFunc("BinlogPlayerMap", func() string { return blm.statsJSON() })
}

func (blm *BinlogPlayerMap) statsJSON() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "{")
	for source, player := range blm.players {
		fmt.Fprintf(buf, "\n \"%v-%v\": %v,", source.KeyRange.Start, source.KeyRange.End, player.statsJSON())
	}
	fmt.Fprintf(buf, "\n \"Enabled\": 1")
	fmt.Fprintf(buf, "\n}")
	return buf.String()
}

func (blm *BinlogPlayerMap) AddPlayer(keyspace string, source topo.SourceShard) {
	bpc, ok := blm.players[source]
	if ok {
		relog.Info("Already playing logs for %v", source)
		return
	}

	bpc = NewBinlogController(blm.ts, blm.dbConfig, keyspace, source)
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
