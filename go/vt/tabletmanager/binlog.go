// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the binlog players launched on masters for filtered
// replication

import (
	"fmt"
	"math/rand" // not crypto-safe is OK here
	"sort"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/concurrency"
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
	cell           string
	keyspaceIdType key.KeyspaceIdType
	keyRange       key.KeyRange
	dbName         string

	// Information about the source
	sourceShard topo.SourceShard

	// interrupted is the channel to close to stop the playback
	interrupted chan struct{}

	// done is the channel to wait for to be sure the player is done
	done chan struct{}

	// stopAtGroupId contains the stopping point for this player, if any
	stopAtGroupId int64

	// BinlogPlayerStats has the stats for the players we're going to use
	binlogPlayerStats *binlogplayer.BinlogPlayerStats
}

func newBinlogPlayerController(ts topo.Server, dbConfig *mysql.ConnectionParams, mysqld *mysqlctl.Mysqld, cell string, keyspaceIdType key.KeyspaceIdType, keyRange key.KeyRange, sourceShard topo.SourceShard, dbName string) *BinlogPlayerController {
	blc := &BinlogPlayerController{
		ts:                ts,
		dbConfig:          dbConfig,
		mysqld:            mysqld,
		cell:              cell,
		keyspaceIdType:    keyspaceIdType,
		keyRange:          keyRange,
		dbName:            dbName,
		sourceShard:       sourceShard,
		binlogPlayerStats: binlogplayer.NewBinlogPlayerStats(),
	}
	return blc
}

func (bpc *BinlogPlayerController) String() string {
	return "BinlogPlayerController(" + bpc.sourceShard.String() + ")"
}

// Start will start the player in the background and run forever
func (bpc *BinlogPlayerController) Start() {
	if bpc.interrupted != nil {
		log.Warningf("%v: already started", bpc)
		return
	}
	log.Infof("%v: starting binlog player", bpc)
	bpc.interrupted = make(chan struct{}, 1)
	bpc.done = make(chan struct{}, 1)
	bpc.stopAtGroupId = 0 // run forever
	go bpc.Loop()
}

// StartUntil will start the Player until we reach the given groupId
func (bpc *BinlogPlayerController) StartUntil(groupId int64) error {
	if bpc.interrupted != nil {
		return fmt.Errorf("%v: already started", bpc)
	}
	log.Infof("%v: starting binlog player until %v", bpc, groupId)
	bpc.interrupted = make(chan struct{}, 1)
	bpc.done = make(chan struct{}, 1)
	bpc.stopAtGroupId = groupId
	go bpc.Loop()
	return nil
}

// WaitForStop will wait until the player is stopped. Use this after StartUntil.
func (bpc *BinlogPlayerController) WaitForStop(waitTimeout time.Duration) error {
	if bpc.interrupted == nil {
		log.Warningf("%v: not started", bpc)
		return fmt.Errorf("WaitForStop called but player not started")
	}
	timer := time.After(waitTimeout)
	select {
	case <-bpc.done:
		bpc.interrupted = nil
		bpc.done = nil
		return nil
	case <-timer:
		bpc.Stop()
		return fmt.Errorf("WaitForStop timeout, stopping current player")
	}
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
	vtClient := binlogplayer.NewDbClient(bpc.dbConfig)
	if err := vtClient.Connect(); err != nil {
		return fmt.Errorf("can't connect to database: %v", err)
	}
	defer vtClient.Close()

	// Read the start position
	startPosition, err := binlogplayer.ReadStartPosition(vtClient, bpc.sourceShard.Uid)
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

	// check which kind of replication we're doing, tables or keyrange
	if len(bpc.sourceShard.Tables) > 0 {
		// tables, first resolve wildcards
		tables, err := bpc.mysqld.ResolveTables(bpc.dbName, bpc.sourceShard.Tables)
		if err != nil {
			return fmt.Errorf("failed to resolve table names: %v", err)
		}

		// tables, just get them
		player := binlogplayer.NewBinlogPlayerTables(vtClient, addr, tables, startPosition, bpc.stopAtGroupId, bpc.binlogPlayerStats)
		return player.ApplyBinlogEvents(bpc.interrupted)
	} else {
		// the data we have to replicate is the intersection of the
		// source keyrange and our keyrange
		overlap, err := key.KeyRangesOverlap(bpc.sourceShard.KeyRange, bpc.keyRange)
		if err != nil {
			return fmt.Errorf("Source shard %v doesn't overlap destination shard %v", bpc.sourceShard.KeyRange, bpc.keyRange)
		}

		player := binlogplayer.NewBinlogPlayerKeyRange(vtClient, addr, bpc.keyspaceIdType, overlap, startPosition, bpc.stopAtGroupId, bpc.binlogPlayerStats)
		return player.ApplyBinlogEvents(bpc.interrupted)
	}
}

func (bpc *BinlogPlayerController) BlpPosition(vtClient *binlogplayer.DBClient) (*blproto.BlpPosition, error) {
	return binlogplayer.ReadStartPosition(vtClient, bpc.sourceShard.Uid)
}

// BinlogPlayerMap controls all the players.
// It can be stopped and restarted.
type BinlogPlayerMap struct {
	// Immutable, set at construction time
	ts       topo.Server
	dbConfig *mysql.ConnectionParams
	mysqld   *mysqlctl.Mysqld

	// This mutex protects the map and the state
	mu      sync.Mutex
	players map[uint32]*BinlogPlayerController
	state   int64
}

const (
	BPM_STATE_RUNNING int64 = iota
	BPM_STATE_STOPPED
)

func NewBinlogPlayerMap(ts topo.Server, dbConfig *mysql.ConnectionParams, mysqld *mysqlctl.Mysqld) *BinlogPlayerMap {
	return &BinlogPlayerMap{
		ts:       ts,
		dbConfig: dbConfig,
		mysqld:   mysqld,
		players:  make(map[uint32]*BinlogPlayerController),
		state:    BPM_STATE_RUNNING,
	}
}

func RegisterBinlogPlayerMap(blm *BinlogPlayerMap) {
	stats.Publish("BinlogPlayerMapSize", stats.IntFunc(blm.size))
	stats.Publish("BinlogPlayerSecondsBehindMaster", stats.IntFunc(func() int64 {
		sbm := int64(0)
		blm.mu.Lock()
		for _, bpc := range blm.players {
			psbm := bpc.binlogPlayerStats.SecondsBehindMaster.Get()
			if psbm > sbm {
				sbm = psbm
			}
		}
		blm.mu.Unlock()
		return sbm
	}))
}

func (blm *BinlogPlayerMap) size() int64 {
	blm.mu.Lock()
	result := len(blm.players)
	blm.mu.Unlock()
	return int64(result)
}

// addPlayer adds a new player to the map. It assumes we have the lock.
func (blm *BinlogPlayerMap) addPlayer(cell string, keyspaceIdType key.KeyspaceIdType, keyRange key.KeyRange, sourceShard topo.SourceShard, dbName string) {
	bpc, ok := blm.players[sourceShard.Uid]
	if ok {
		log.Infof("Already playing logs for %v", sourceShard)
		return
	}

	bpc = newBinlogPlayerController(blm.ts, blm.dbConfig, blm.mysqld, cell, keyspaceIdType, keyRange, sourceShard, dbName)
	blm.players[sourceShard.Uid] = bpc
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
	blm.players = make(map[uint32]*BinlogPlayerController)
	blm.mu.Unlock()

	if hadPlayers {
		blm.enableSuperToSetTimestamp()
	}
}

// RefreshMap reads the right data from topo.Server and makes sure
// we're playing the right logs.
func (blm *BinlogPlayerMap) RefreshMap(tablet topo.Tablet, keyspaceInfo *topo.KeyspaceInfo, shardInfo *topo.ShardInfo) {
	log.Infof("Refreshing map of binlog players")
	if keyspaceInfo == nil {
		log.Warningf("Could not read keyspaceInfo, not changing anything")
		return
	}

	if shardInfo == nil {
		log.Warningf("Could not read shardInfo, not changing anything")
		return
	}

	blm.mu.Lock()

	// get the existing sources and build a map of sources to remove
	toRemove := make(map[uint32]bool)
	hadPlayers := false
	for source := range blm.players {
		toRemove[source] = true
		hadPlayers = true
	}

	// for each source, add it if not there, and delete from toRemove
	for _, sourceShard := range shardInfo.SourceShards {
		blm.addPlayer(tablet.Alias.Cell, keyspaceInfo.ShardingColumnType, tablet.KeyRange, sourceShard, tablet.DbName())
		delete(toRemove, sourceShard.Uid)
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
func (blm *BinlogPlayerMap) BlpPositionList() (*blproto.BlpPositionList, error) {
	// create a db connection for this purpose
	vtClient := binlogplayer.NewDbClient(blm.dbConfig)
	if err := vtClient.Connect(); err != nil {
		return nil, fmt.Errorf("can't connect to database: %v", err)
	}
	defer vtClient.Close()

	result := &blproto.BlpPositionList{}
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

// RunUntil will run all the players until they reach the given position.
// Holds the map lock during that exercise, shouldn't take long at all.
func (blm *BinlogPlayerMap) RunUntil(blpPositionList *blproto.BlpPositionList, waitTimeout time.Duration) error {
	// lock and check state
	blm.mu.Lock()
	defer blm.mu.Unlock()
	if blm.state != BPM_STATE_STOPPED {
		return fmt.Errorf("RunUntil: player not stopped: %v", blm.state)
	}
	log.Infof("Starting map of binlog players until position")

	// find the exact stop position for all players, to be sure
	// we're not doing anything wrong
	groupIds := make(map[uint32]int64)
	for _, bpc := range blm.players {
		pos, err := blpPositionList.FindBlpPositionById(bpc.sourceShard.Uid)
		if err != nil {
			return fmt.Errorf("No binlog position passed in for player Uid %v", bpc.sourceShard.Uid)
		}
		groupIds[bpc.sourceShard.Uid] = pos.GroupId
	}

	// start all the players giving them where to stop
	for _, bpc := range blm.players {
		if err := bpc.StartUntil(groupIds[bpc.sourceShard.Uid]); err != nil {
			return err
		}
	}

	// wait for all players to be stopped, or timeout
	wg := sync.WaitGroup{}
	rec := concurrency.AllErrorRecorder{}
	for _, bpc := range blm.players {
		wg.Add(1)
		go func(bpc *BinlogPlayerController) {
			if err := bpc.WaitForStop(waitTimeout); err != nil {
				rec.RecordError(err)
			}
			wg.Done()
		}(bpc)
	}
	wg.Wait()

	return rec.Error()
}

// The following structures are used for status display

// BinlogPlayerControllerStatus is the status of an individual controller
type BinlogPlayerControllerStatus struct {
	// configuration values
	Index         uint32
	SourceShard   topo.SourceShard
	StopAtGroupId int64

	// stats and current values
	LastGroupId         int64
	SecondsBehindMaster int64
	Counts              map[string]int64
	Rates               map[string][]float64
}

// BinlogPlayerControllerStatusList is the list of statuses
type BinlogPlayerControllerStatusList []*BinlogPlayerControllerStatus

// Len is part of sort.Interface
func (bpcsl BinlogPlayerControllerStatusList) Len() int {
	return len(bpcsl)
}

// Less is part of sort.Interface
func (bpcsl BinlogPlayerControllerStatusList) Less(i, j int) bool {
	return bpcsl[i].Index < bpcsl[j].Index
}

// Swap is part of sort.Interface
func (bpcsl BinlogPlayerControllerStatusList) Swap(i, j int) {
	bpcsl[i], bpcsl[j] = bpcsl[j], bpcsl[i]
}

// BinlogPlayerMapStatus is the complete player status
type BinlogPlayerMapStatus struct {
	State       string
	Controllers BinlogPlayerControllerStatusList
}

// Status returns the BinlogPlayerMapStatus for the BinlogPlayerMap
func (blm *BinlogPlayerMap) Status() *BinlogPlayerMapStatus {
	// Create the result, take care of the stopped state.
	result := &BinlogPlayerMapStatus{}
	blm.mu.Lock()
	defer blm.mu.Unlock()

	// fill in state
	if blm.state == BPM_STATE_STOPPED {
		result.State = "Stopped"
	} else {
		result.State = "Running"
	}

	// fill in controllers
	result.Controllers = make([]*BinlogPlayerControllerStatus, 0, len(blm.players))

	for i, bpc := range blm.players {
		bpcs := &BinlogPlayerControllerStatus{
			Index:               i,
			SourceShard:         bpc.sourceShard,
			StopAtGroupId:       bpc.stopAtGroupId,
			LastGroupId:         bpc.binlogPlayerStats.LastGroupId.Get(),
			SecondsBehindMaster: bpc.binlogPlayerStats.SecondsBehindMaster.Get(),
			Counts:              bpc.binlogPlayerStats.Timings.Counts(),
			Rates:               bpc.binlogPlayerStats.Rates.Get(),
		}
		result.Controllers = append(result.Controllers, bpcs)
	}
	sort.Sort(result.Controllers)

	return result
}
