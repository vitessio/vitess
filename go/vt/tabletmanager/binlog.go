// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

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

	// done is the channel to wait for to be sure the player is done
	done chan struct{}
}

func NewBinlogPlayerController(ts topo.Server, dbConfig *mysql.ConnectionParams, mysqld *mysqlctl.Mysqld, cell string, keyRange key.KeyRange, sourceShard topo.SourceShard) *BinlogPlayerController {
	blc := &BinlogPlayerController{
		ts:          ts,
		dbConfig:    dbConfig,
		mysqld:      mysqld,
		cell:        cell,
		keyRange:    keyRange,
		sourceShard: sourceShard,
	}
	return blc
}

func (bpc *BinlogPlayerController) String() string {
	return "BinlogPlayerController(" + bpc.sourceShard.String() + ")"
}

func (bpc *BinlogPlayerController) Start() {
	if bpc.interrupted != nil {
		log.Warningf("%v: already started", bpc)
		return
	}
	log.Infof("%v: starting binlog player", bpc)
	bpc.interrupted = make(chan struct{}, 1)
	bpc.done = make(chan struct{}, 1)
	go bpc.Loop()
}

func (bpc *BinlogPlayerController) Stop() {
	if bpc.interrupted == nil {
		log.Warningf("%v: not started", bpc)
		return
	}
	log.Infof("%v: stopping binlog player", bpc)
	close(bpc.interrupted)
	select {
	case <-bpc.done:
		bpc.interrupted = nil
		bpc.done = nil
		return
	}
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

	log.Infof("%v: exited main binlog player loop", bpc)
	close(bpc.done)
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
			log.Errorf("%v: caught panic: %v", bpc, x)
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
	newServerIndex := rand.Intn(len(addrs.Entries))
	addr := fmt.Sprintf("%v:%v", addrs.Entries[newServerIndex].Host, addrs.Entries[newServerIndex].NamedPortMap["_vtocc"])

	// the data we have to replicate is the intersection of the
	// source keyrange and our keyrange
	overlap, err := key.KeyRangesOverlap(bpc.sourceShard.KeyRange, bpc.keyRange)
	if err != nil {
		return fmt.Errorf("Source shard %v doesn't overlap destination shard %v", bpc.sourceShard.KeyRange, bpc.keyRange)
	}

	player := mysqlctl.NewBinlogPlayer(vtClient, addr, overlap, startPosition)
	return player.ApplyBinlogEvents(bpc.interrupted)
}

func (bpc *BinlogPlayerController) BlpPosition(vtClient *mysqlctl.DBClient) (*mysqlctl.BlpPosition, error) {
	return mysqlctl.ReadStartPosition(vtClient, bpc.sourceShard.Uid)
}

// BinlogPlayerMap controls all the players.
// It can be stopped and restarted.
type BinlogPlayerMap struct {
	// Immutable, set at construction time
	ts       topo.Server
	dbConfig mysql.ConnectionParams
	mysqld   *mysqlctl.Mysqld

	// This mutex protects the map and the state
	mu      sync.Mutex
	players map[topo.SourceShard]*BinlogPlayerController
	state   int64
}

const (
	BPM_STATE_RUNNING int64 = iota
	BPM_STATE_STOPPED
)

func NewBinlogPlayerMap(ts topo.Server, dbConfig mysql.ConnectionParams, mysqld *mysqlctl.Mysqld) *BinlogPlayerMap {
	return &BinlogPlayerMap{
		ts:       ts,
		dbConfig: dbConfig,
		mysqld:   mysqld,
		players:  make(map[topo.SourceShard]*BinlogPlayerController),
		state:    BPM_STATE_RUNNING,
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
	if blm.state == BPM_STATE_RUNNING {
		bpc.Start()
	}
}

// StopAllPlayersAndReset stops all the binlog players, and reset the map of
// players.
func (blm *BinlogPlayerMap) StopAllPlayersAndReset() {
	hadPlayers := false
	blm.mu.Lock()
	for _, bpc := range blm.players {
		if blm.state == BPM_STATE_RUNNING {
			bpc.Stop()
		}
		hadPlayers = true
	}
	blm.players = make(map[topo.SourceShard]*BinlogPlayerController)
	blm.mu.Unlock()

	if hadPlayers {
		blm.enableSuperToSetTimestamp()
	}
}

// RefreshMap reads the right data from topo.Server and makes sure
// we're playing the right logs.
func (blm *BinlogPlayerMap) RefreshMap(tablet topo.Tablet, shardInfo *topo.ShardInfo) {
	log.Infof("Refreshing map of binlog players")
	if shardInfo == nil {
		log.Warningf("Could not read shardInfo, not changing anything")
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
		blm.addPlayer(tablet.Alias.Cell, tablet.KeyRange, sourceShard)
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
		blm.enableSuperToSetTimestamp()
	}
}

// After this is called, the clients will need super privileges
// to set timestamp
func (blm *BinlogPlayerMap) enableSuperToSetTimestamp() {
	if err := blm.mysqld.ExecuteMysqlCommand("set @@global.super_to_set_timestamp = 1"); err != nil {
		log.Warningf("Cannot set super_to_set_timestamp=1: %v", err)
	} else {
		log.Info("Successfully set super_to_set_timestamp=1")
	}
}

// Stop stops the current players, but does not remove them from the map.
// Call 'Start' to restart the playback.
func (blm *BinlogPlayerMap) Stop() {
	blm.mu.Lock()
	defer blm.mu.Unlock()
	if blm.state == BPM_STATE_STOPPED {
		log.Warningf("BinlogPlayerMap already stopped")
		return
	}
	log.Infof("Stopping map of binlog players")
	for _, bpc := range blm.players {
		bpc.Stop()
	}
	blm.state = BPM_STATE_STOPPED
}

// Start restarts the current players
func (blm *BinlogPlayerMap) Start() {
	blm.mu.Lock()
	defer blm.mu.Unlock()
	if blm.state == BPM_STATE_RUNNING {
		log.Warningf("BinlogPlayerMap already started")
		return
	}
	log.Infof("Starting map of binlog players")
	for _, bpc := range blm.players {
		bpc.Start()
	}
	blm.state = BPM_STATE_RUNNING
}

// BlpPositionList returns the current position of all the players
func (blm *BinlogPlayerMap) BlpPositionList() (*BlpPositionList, error) {
	// create a db connection for this purpose
	vtClient := mysqlctl.NewDbClient(&blm.dbConfig)
	if err := vtClient.Connect(); err != nil {
		return nil, fmt.Errorf("can't connect to database: %v", err)
	}
	defer vtClient.Close()

	result := &BlpPositionList{}
	blm.mu.Lock()
	defer blm.mu.Unlock()
	for _, bpc := range blm.players {
		blp, err := bpc.BlpPosition(vtClient)
		if err != nil {
			return nil, fmt.Errorf("can't read current position for %v: %v", bpc, err)
		}

		result.Entries = append(result.Entries, *blp)
	}
	return result, nil
}
