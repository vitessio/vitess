// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttablet

// This file handles the binlog players launched on masters for filtered
// replication

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo"
)

const (
	BINLOG_PLAYER_CONNECTING = iota
	BINLOG_PLAYER_PLAYING
	BINLOG_PLAYER_SLEEPING
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// BinlogPlayerController controls one player
type BinlogPlayerController struct {
	ts       topo.Server
	dbConfig *mysql.ConnectionParams

	// Information about us
	cell     string
	keyRange key.KeyRange

	// Information about the source
	sourceKeyspace string
	sourceShard    topo.SourceShard

	// Player is the BinlogPlayer when we have one, protected by mu
	player *mysqlctl.BinlogPlayer
	mu     sync.Mutex

	// states is our state accounting variable, for stats
	states *stats.States

	// interrupted is the channel to close to stop the playback
	interrupted chan struct{}
}

func NewBinlogPlayerController(ts topo.Server, dbConfig *mysql.ConnectionParams, cell string, keyRange key.KeyRange, sourceKeyspace string, sourceShard topo.SourceShard) *BinlogPlayerController {
	blc := &BinlogPlayerController{
		ts:             ts,
		dbConfig:       dbConfig,
		cell:           cell,
		keyRange:       keyRange,
		sourceKeyspace: sourceKeyspace,
		sourceShard:    sourceShard,
		interrupted:    make(chan struct{}, 1),
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

func (bpc *BinlogPlayerController) String() string {
	return fmt.Sprintf("BinlogPlayerController(%v/%v-%v)", bpc.sourceKeyspace, bpc.sourceShard.KeyRange.Start.Hex(), bpc.sourceShard.KeyRange.End.Hex())
}

func (bpc *BinlogPlayerController) Start() {
	log.Infof("%v: Starting binlog player", bpc)
	go bpc.Loop()
}

func (bpc *BinlogPlayerController) Stop() {
	log.Infof("%v: Stopping binlog player", bpc)
	close(bpc.interrupted)
}

func (bpc *BinlogPlayerController) Loop() {
	for {
		err := bpc.Iteration()
		if err == nil {
			// this happens when we get interrupted
			break
		}
		log.Warningf("%v: %v", bpc, err)

		// sleep for a bit before retrying to connect
		bpc.states.SetState(BINLOG_PLAYER_SLEEPING)
		time.Sleep(5 * time.Second)
	}

	log.Infof("%v: Exited main binlog player loop", bpc)
}

func (bpc *BinlogPlayerController) Iteration() (err error) {
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("%v: Caught panic: %v", bpc, x)
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
	startPosition, err := mysqlctl.ReadStartPosition(vtClient, bpc.sourceShard.KeyRange)
	if err != nil {
		return fmt.Errorf("can't read startPosition: %v", err)
	}

	// Find the server list for the source shard in our cell
	sourceShardName := string(bpc.sourceShard.KeyRange.Start.Hex()) + "-" + string(bpc.sourceShard.KeyRange.End.Hex())
	addrs, err := bpc.ts.GetSrvTabletType(bpc.cell, bpc.sourceKeyspace, sourceShardName, topo.TYPE_REPLICA)
	if err != nil {
		return fmt.Errorf("can't find any source tablet for %v %v %v %v: %v", bpc.cell, bpc.sourceKeyspace, sourceShardName, topo.TYPE_REPLICA, err)
	}
	if len(addrs.Entries) == 0 {
		return fmt.Errorf("empty source tablet list for %v %v %v %v", bpc.cell, bpc.sourceKeyspace, sourceShardName, topo.TYPE_REPLICA)
	}

	// if the server we were using before is in the list, just keep using it
	usePreviousServer := false
	for _, addr := range addrs.Entries {
		vtAddr := fmt.Sprintf("%v:%v", addr.Host, addr.NamedPortMap["_vtocc"])
		if vtAddr == startPosition.Addr {
			log.Infof("%v: Previous server %s still healthy, using it", bpc, vtAddr)
			usePreviousServer = true
		}
	}

	// if we can't use the previous server, pick a new one randomely
	if !usePreviousServer {
		newServerIndex := rand.Intn(len(addrs.Entries))
		startPosition.Addr = fmt.Sprintf("%v:%v", addrs.Entries[newServerIndex].Host, addrs.Entries[newServerIndex].NamedPortMap["_vtocc"])
		startPosition.Position.MasterFilename = ""
		startPosition.Position.MasterPosition = 0
		log.Infof("%v: Connecting to different server: %s", bpc, startPosition.Addr)
	}

	// the data we have to replicate is the intersection of the
	// source keyrange and our keyrange
	overlap, err := key.KeyRangesOverlap(bpc.sourceShard.KeyRange, bpc.keyRange)
	if err != nil {
		return fmt.Errorf("Source shard %v doesn't overlap destination shard %v", bpc.sourceShard.KeyRange, bpc.keyRange)
	}
	startPosition.KeyRange = overlap

	// Create the player.
	bpc.mu.Lock()
	bpc.player, err = mysqlctl.NewBinlogPlayer(vtClient, startPosition, bpc.sourceShard.KeyRange, nil /*tables*/, 1 /*txnBatch*/, 30*time.Second /*maxTxnInterval*/, false /*execDdl*/)
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
	stats.PublishJSONFunc("BinlogPlayerMap", blm.statsJSON)
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

func (blm *BinlogPlayerMap) AddPlayer(cell string, keyRange key.KeyRange, sourceKeyspace string, sourceShard topo.SourceShard) {
	bpc, ok := blm.players[sourceShard]
	if ok {
		log.Infof("Already playing logs for %v", sourceShard)
		return
	}

	bpc = NewBinlogPlayerController(blm.ts, blm.dbConfig, cell, keyRange, sourceKeyspace, sourceShard)
	blm.players[sourceShard] = bpc
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
	log.Infof("Refreshing map of binlog players")

	// read the shard to get SourceShards
	shardInfo, err := blm.ts.GetShard(tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Errorf("Cannot read shard for this tablet: %v", tablet.Alias())
		return
	}

	// get the existing sources and build a map of sources to remove
	toRemove := make(map[topo.SourceShard]bool)
	for source, _ := range blm.players {
		toRemove[source] = true
	}

	// for each source, add it if not there, and delete from toRemove
	for _, sourceShard := range shardInfo.SourceShards {
		blm.AddPlayer(tablet.Cell, tablet.KeyRange, tablet.Keyspace, sourceShard)
		delete(toRemove, sourceShard)
	}

	// remove all entries from toRemove
	for source, _ := range toRemove {
		blm.players[source].Stop()
		delete(blm.players, source)
	}
}
