// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttablet

// This file handles the binlog players launched on masters for filtered
// replication

import (
	"fmt"
	"math/rand" // not crypto-safe is OK here
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// BinlogPlayerController controls one player
type BinlogPlayerController struct {
	ts       topo.Server
	dbConfig *mysql.ConnectionParams
	mysqld   *mysqlctl.Mysqld

	// Information about us
	cell     string
	keyRange key.KeyRange

	// Information about the source
	sourceShard topo.SourceShard

	// interrupted is the channel to close to stop the playback
	interrupted chan struct{}
}

func NewBinlogPlayerController(ts topo.Server, dbConfig *mysql.ConnectionParams, mysqld *mysqlctl.Mysqld, cell string, keyRange key.KeyRange, sourceShard topo.SourceShard) *BinlogPlayerController {
	blc := &BinlogPlayerController{
		ts:          ts,
		dbConfig:    dbConfig,
		mysqld:      mysqld,
		cell:        cell,
		keyRange:    keyRange,
		sourceShard: sourceShard,
		interrupted: make(chan struct{}, 1),
	}
	return blc
}

func (bpc *BinlogPlayerController) String() string {
	return "BinlogPlayerController(" + bpc.sourceShard.String() + ")"
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
		time.Sleep(5 * time.Second)
	}

	log.Infof("%v: Exited main binlog player loop", bpc)
}

func (bpc *BinlogPlayerController) DisableSuperToSetTimestamp() {
	if err := bpc.mysqld.ExecuteMysqlCommand("set @@global.super_to_set_timestamp = 0"); err != nil {
		log.Warningf("Cannot set super_to_set_timestamp=0: %v", err)
	} else {
		log.Info("Successfully set super_to_set_timestamp=0")
	}
}

func (bpc *BinlogPlayerController) Iteration() (err error) {
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("%v: Caught panic: %v", bpc, x)
			err = fmt.Errorf("panic: %v", x)
		}
	}()

	// Enable any user to set the timestamp.
	// We do it on every iteration to be sure, in case mysql was
	// restarted.
	bpc.DisableSuperToSetTimestamp()

	// create the db connection, connect it
	vtClient := mysqlctl.NewDbClient(bpc.dbConfig)
	if err := vtClient.Connect(); err != nil {
		return fmt.Errorf("can't connect to database: %v", err)
	}
	defer vtClient.Close()

	// Read the start position
	startPosition, err := mysqlctl.ReadStartPosition(vtClient, bpc.sourceShard.Uid)
	if err != nil {
		return fmt.Errorf("can't read startPosition: %v", err)
	}

	// Find the server list for the source shard in our cell
	addrs, err := bpc.ts.GetEndPoints(bpc.cell, bpc.sourceShard.Keyspace, bpc.sourceShard.Shard, topo.TYPE_REPLICA)
	if err != nil {
		return fmt.Errorf("can't find any source tablet for %v %v %v: %v", bpc.cell, bpc.sourceShard.String(), topo.TYPE_REPLICA, err)
	}
	if len(addrs.Entries) == 0 {
		return fmt.Errorf("empty source tablet list for %v %v %v", bpc.cell, bpc.sourceShard.String(), topo.TYPE_REPLICA)
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

	// if we can't use the previous server, pick a new one randomly
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

	// Create the player.
	player, err := mysqlctl.NewBinlogPlayer(vtClient, overlap, bpc.sourceShard.Uid, startPosition, nil /*tables*/, 1 /*txnBatch*/, 30*time.Second /*maxTxnInterval*/, false /*execDdl*/)
	if err != nil {
		return fmt.Errorf("can't create player: %v", err)
	}

	// Run player loop until it's done.
	return player.ApplyBinlogEvents(bpc.interrupted)
}

// BinlogPlayerMap controls all the players
type BinlogPlayerMap struct {
	ts       topo.Server
	dbConfig mysql.ConnectionParams
	mysqld   *mysqlctl.Mysqld

	// This mutex protects the map
	mu      sync.Mutex
	players map[topo.SourceShard]*BinlogPlayerController
}

func NewBinlogPlayerMap(ts topo.Server, dbConfig mysql.ConnectionParams, mysqld *mysqlctl.Mysqld) *BinlogPlayerMap {
	return &BinlogPlayerMap{
		ts:       ts,
		dbConfig: dbConfig,
		mysqld:   mysqld,
		players:  make(map[topo.SourceShard]*BinlogPlayerController),
	}
}

func RegisterBinlogPlayerMap(blm *BinlogPlayerMap) {
	stats.Publish("BinlogPlayerMapSize", stats.IntFunc(blm.size))
}

func (blm *BinlogPlayerMap) size() int64 {
	blm.mu.Lock()
	result := len(blm.players)
	blm.mu.Unlock()
	return int64(result)
}

// addPlayer adds a new player to the map. It assumes we have the lock.
func (blm *BinlogPlayerMap) addPlayer(cell string, keyRange key.KeyRange, sourceShard topo.SourceShard) {
	bpc, ok := blm.players[sourceShard]
	if ok {
		log.Infof("Already playing logs for %v", sourceShard)
		return
	}

	bpc = NewBinlogPlayerController(blm.ts, &blm.dbConfig, blm.mysqld, cell, keyRange, sourceShard)
	blm.players[sourceShard] = bpc
	bpc.Start()
}

func (blm *BinlogPlayerMap) StopAllPlayers() {
	hadPlayers := false
	blm.mu.Lock()
	for _, bpc := range blm.players {
		bpc.Stop()
		hadPlayers = true
	}
	blm.players = make(map[topo.SourceShard]*BinlogPlayerController)
	blm.mu.Unlock()

	if hadPlayers {
		blm.EnableSuperToSetTimestamp()
	}
}

// RefreshMap reads the right data from topo.Server and makes sure
// we're playing the right logs
func (blm *BinlogPlayerMap) RefreshMap(tablet topo.Tablet, shardInfo *topo.ShardInfo) {
	log.Infof("Refreshing map of binlog players")

	if shardInfo == nil {
		return
	}

	blm.mu.Lock()

	// get the existing sources and build a map of sources to remove
	toRemove := make(map[topo.SourceShard]bool)
	hadPlayers := false
	for source := range blm.players {
		toRemove[source] = true
		hadPlayers = true
	}

	// for each source, add it if not there, and delete from toRemove
	for _, sourceShard := range shardInfo.SourceShards {
		blm.addPlayer(tablet.Cell, tablet.KeyRange, sourceShard)
		delete(toRemove, sourceShard)
	}
	hasPlayers := len(shardInfo.SourceShards) > 0

	// remove all entries from toRemove
	for source := range toRemove {
		blm.players[source].Stop()
		delete(blm.players, source)
	}

	blm.mu.Unlock()

	if hadPlayers && !hasPlayers {
		blm.EnableSuperToSetTimestamp()
	}
}

// After this is called, the clients will need super privileges
// to set timestamp
func (blm *BinlogPlayerMap) EnableSuperToSetTimestamp() {
	if err := blm.mysqld.ExecuteMysqlCommand("set @@global.super_to_set_timestamp = 1"); err != nil {
		log.Warningf("Cannot set super_to_set_timestamp=1: %v", err)
	} else {
		log.Info("Successfully set super_to_set_timestamp=1")
	}
}
