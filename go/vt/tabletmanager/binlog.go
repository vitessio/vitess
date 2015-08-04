// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the binlog players launched on masters for filtered
// replication

import (
	"fmt"
	"html/template"
	"math/rand" // not crypto-safe is OK here
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// BinlogPlayerController controls one player.
type BinlogPlayerController struct {
	// Configuration parameters (set at construction, immutable).
	ts       topo.Server
	dbConfig *sqldb.ConnParams
	mysqld   mysqlctl.MysqlDaemon

	// Information about us (set at construction, immutable).
	cell           string
	keyspaceIDType pb.KeyspaceIdType
	keyRange       *pb.KeyRange
	dbName         string

	// Information about the source (set at construction, immutable).
	sourceShard *pb.Shard_SourceShard

	// BinlogPlayerStats has the stats for the players we're going to use
	// (pointer is set at construction, immutable, values are thread-safe).
	binlogPlayerStats *binlogplayer.BinlogPlayerStats

	// playerMutex is used to protect the next fields in this structure.
	// They will change depending on our state.
	playerMutex sync.Mutex

	// ctx is the context to cancel to stop the playback.
	ctx    context.Context
	cancel context.CancelFunc

	// done is the channel to wait for to be sure the player is done.
	done chan struct{}

	// stopPosition contains the stopping point for this player, if any.
	stopPosition myproto.ReplicationPosition

	// information about the individual tablet we're replicating from.
	sourceTablet topo.TabletAlias

	// last error we've seen by the player.
	lastError error
}

func newBinlogPlayerController(ts topo.Server, dbConfig *sqldb.ConnParams, mysqld mysqlctl.MysqlDaemon, cell string, keyspaceIDType pb.KeyspaceIdType, keyRange *pb.KeyRange, sourceShard *pb.Shard_SourceShard, dbName string) *BinlogPlayerController {
	blc := &BinlogPlayerController{
		ts:                ts,
		dbConfig:          dbConfig,
		mysqld:            mysqld,
		cell:              cell,
		keyspaceIDType:    keyspaceIDType,
		keyRange:          keyRange,
		dbName:            dbName,
		sourceShard:       sourceShard,
		binlogPlayerStats: binlogplayer.NewBinlogPlayerStats(),
	}
	return blc
}

func (bpc *BinlogPlayerController) String() string {
	return "BinlogPlayerController(" + topo.SourceShardString(bpc.sourceShard) + ")"
}

// Start will start the player in the background and run forever.
func (bpc *BinlogPlayerController) Start(ctx context.Context) {
	bpc.playerMutex.Lock()
	defer bpc.playerMutex.Unlock()
	if bpc.ctx != nil {
		log.Warningf("%v: already started", bpc)
		return
	}
	log.Infof("%v: starting binlog player", bpc)
	bpc.ctx, bpc.cancel = context.WithCancel(ctx)
	bpc.done = make(chan struct{}, 1)
	bpc.stopPosition = myproto.ReplicationPosition{} // run forever
	go bpc.Loop()
}

// StartUntil will start the Player until we reach the given position.
func (bpc *BinlogPlayerController) StartUntil(ctx context.Context, stopPos myproto.ReplicationPosition) error {
	bpc.playerMutex.Lock()
	defer bpc.playerMutex.Unlock()
	if bpc.ctx != nil {
		return fmt.Errorf("%v: already started", bpc)
	}
	log.Infof("%v: starting binlog player until %v", bpc, stopPos)
	bpc.ctx, bpc.cancel = context.WithCancel(ctx)
	bpc.done = make(chan struct{}, 1)
	bpc.stopPosition = stopPos
	go bpc.Loop()
	return nil
}

// reset will clear the internal data structures.
func (bpc *BinlogPlayerController) reset() {
	bpc.playerMutex.Lock()
	defer bpc.playerMutex.Unlock()
	bpc.ctx = nil
	bpc.cancel = nil
	bpc.done = nil
	bpc.sourceTablet = topo.TabletAlias{}
	bpc.lastError = nil
}

// WaitForStop will wait until the player is stopped. Use this after StartUntil.
func (bpc *BinlogPlayerController) WaitForStop(waitTimeout time.Duration) error {
	// take the lock, check the data, get what we need, release the lock.
	bpc.playerMutex.Lock()
	if bpc.ctx == nil {
		bpc.playerMutex.Unlock()
		log.Warningf("%v: not started", bpc)
		return fmt.Errorf("WaitForStop called but player not started")
	}
	done := bpc.done
	bpc.playerMutex.Unlock()

	// start waiting
	tmr := time.NewTimer(waitTimeout)
	defer tmr.Stop()
	select {
	case <-done:
		bpc.reset()
		return nil
	case <-tmr.C:
		bpc.Stop()
		return fmt.Errorf("WaitForStop timeout, stopping current player")
	}
}

// Stop will ask the controller to stop playing, and wait until it is stopped.
func (bpc *BinlogPlayerController) Stop() {
	bpc.playerMutex.Lock()
	if bpc.ctx == nil {
		bpc.playerMutex.Unlock()
		log.Warningf("%v: not started", bpc)
		return
	}
	log.Infof("%v: stopping binlog player", bpc)
	bpc.cancel()
	done := bpc.done
	bpc.playerMutex.Unlock()

	select {
	case <-done:
		bpc.reset()
	}
}

// Loop runs the main player loop: try to play, and in case of error,
// sleep for 5 seconds and try again.
func (bpc *BinlogPlayerController) Loop() {
	for {
		err := bpc.Iteration()
		if err == nil {
			// this happens when we get interrupted
			break
		}
		log.Warningf("%v: %v", bpc, err)

		// clear the source, remember the error
		bpc.playerMutex.Lock()
		bpc.sourceTablet = topo.TabletAlias{}
		bpc.lastError = err
		bpc.playerMutex.Unlock()

		// sleep for a bit before retrying to connect
		time.Sleep(5 * time.Second)
	}

	log.Infof("%v: exited main binlog player loop", bpc)
	close(bpc.done)
}

// Iteration is a single iteration for the player: get the current status,
// try to play, and plays until interrupted, or until an error occurs.
func (bpc *BinlogPlayerController) Iteration() (err error) {
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("%v: caught panic: %v", bpc, x)
			err = fmt.Errorf("panic: %v", x)
		}
	}()

	// Apply any special settings necessary for playback of binlogs.
	// We do it on every iteration to be sure, in case MySQL was restarted.
	if err := bpc.mysqld.EnableBinlogPlayback(); err != nil {
		// We failed to apply the required settings, so we shouldn't keep going.
		return err
	}

	// create the db connection, connect it
	vtClient := binlogplayer.NewDbClient(bpc.dbConfig)
	if err := vtClient.Connect(); err != nil {
		return fmt.Errorf("can't connect to database: %v", err)
	}
	defer vtClient.Close()

	// Read the start position
	startPosition, flags, err := binlogplayer.ReadStartPosition(vtClient, bpc.sourceShard.Uid)
	if err != nil {
		return fmt.Errorf("can't read startPosition: %v", err)
	}

	// if we shouldn't start, we just error out and try again later
	if strings.Index(flags, binlogplayer.BlpFlagDontStart) != -1 {
		return fmt.Errorf("not starting because flag '%v' is set", binlogplayer.BlpFlagDontStart)
	}

	// Find the server list for the source shard in our cell
	addrs, _, err := bpc.ts.GetEndPoints(bpc.ctx, bpc.cell, bpc.sourceShard.Keyspace, bpc.sourceShard.Shard, topo.TYPE_REPLICA)
	if err != nil {
		// If this calls fails because the context was canceled,
		// we need to return nil.
		select {
		case <-bpc.ctx.Done():
			if bpc.ctx.Err() == context.Canceled {
				return nil
			}
		default:
		}
		return fmt.Errorf("can't find any source tablet for %v %v %v: %v", bpc.cell, bpc.sourceShard.String(), topo.TYPE_REPLICA, err)
	}
	if len(addrs.Entries) == 0 {
		return fmt.Errorf("empty source tablet list for %v %v %v", bpc.cell, bpc.sourceShard.String(), topo.TYPE_REPLICA)
	}
	newServerIndex := rand.Intn(len(addrs.Entries))
	endPoint := addrs.Entries[newServerIndex]

	// save our current server
	bpc.playerMutex.Lock()
	bpc.sourceTablet = topo.TabletAlias{
		Cell: bpc.cell,
		Uid:  addrs.Entries[newServerIndex].Uid,
	}
	bpc.lastError = nil
	bpc.playerMutex.Unlock()

	// check which kind of replication we're doing, tables or keyrange
	if len(bpc.sourceShard.Tables) > 0 {
		// tables, first resolve wildcards
		tables, err := mysqlctl.ResolveTables(bpc.mysqld, bpc.dbName, bpc.sourceShard.Tables)
		if err != nil {
			return fmt.Errorf("failed to resolve table names: %v", err)
		}

		// tables, just get them
		player := binlogplayer.NewBinlogPlayerTables(vtClient, endPoint, tables, startPosition, bpc.stopPosition, bpc.binlogPlayerStats)
		return player.ApplyBinlogEvents(bpc.ctx)
	}
	// the data we have to replicate is the intersection of the
	// source keyrange and our keyrange
	overlap, err := key.KeyRangesOverlap3(bpc.sourceShard.KeyRange, bpc.keyRange)
	if err != nil {
		return fmt.Errorf("Source shard %v doesn't overlap destination shard %v", bpc.sourceShard.KeyRange, bpc.keyRange)
	}

	player := binlogplayer.NewBinlogPlayerKeyRange(vtClient, endPoint, bpc.keyspaceIDType, overlap, startPosition, bpc.stopPosition, bpc.binlogPlayerStats)
	return player.ApplyBinlogEvents(bpc.ctx)
}

// BlpPosition returns the current position for a controller, as read from the database.
func (bpc *BinlogPlayerController) BlpPosition(vtClient *binlogplayer.DBClient) (*blproto.BlpPosition, string, error) {
	return binlogplayer.ReadStartPosition(vtClient, bpc.sourceShard.Uid)
}

// BinlogPlayerMap controls all the players.
// It can be stopped and restarted.
type BinlogPlayerMap struct {
	// Immutable, set at construction time.
	ts       topo.Server
	dbConfig *sqldb.ConnParams
	mysqld   mysqlctl.MysqlDaemon

	// This mutex protects the map and the state.
	mu      sync.Mutex
	players map[uint32]*BinlogPlayerController
	state   int64
}

const (
	// BpmStateRunning indicates BinlogPlayerMap is running.
	BpmStateRunning int64 = iota
	// BpmStateStopped indicates BinlogPlayerMap has stopped.
	BpmStateStopped
)

// NewBinlogPlayerMap creates a new map of players.
func NewBinlogPlayerMap(ts topo.Server, dbConfig *sqldb.ConnParams, mysqld mysqlctl.MysqlDaemon) *BinlogPlayerMap {
	return &BinlogPlayerMap{
		ts:       ts,
		dbConfig: dbConfig,
		mysqld:   mysqld,
		players:  make(map[uint32]*BinlogPlayerController),
		state:    BpmStateRunning,
	}
}

// RegisterBinlogPlayerMap registers the varz for the players.
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
	stats.Publish("BinlogPlayerSecondsBehindMasterMap", stats.CountersFunc(func() map[string]int64 {
		blm.mu.Lock()
		result := make(map[string]int64, len(blm.players))
		for i, bpc := range blm.players {
			sbm := bpc.binlogPlayerStats.SecondsBehindMaster.Get()
			result[fmt.Sprintf("%v", i)] = sbm
		}
		blm.mu.Unlock()
		return result
	}))
	stats.Publish("BinlogPlayerSourceShardNameMap", stats.StringMapFunc(func() map[string]string {
		blm.mu.Lock()
		result := make(map[string]string, len(blm.players))
		for i, bpc := range blm.players {
			name := bpc.sourceShard.Keyspace + "/" + bpc.sourceShard.Shard
			result[fmt.Sprintf("%v", i)] = name
		}
		blm.mu.Unlock()
		return result
	}))
	stats.Publish("BinlogPlayerSourceTabletAliasMap", stats.StringMapFunc(func() map[string]string {
		blm.mu.Lock()
		result := make(map[string]string, len(blm.players))
		for i, bpc := range blm.players {
			bpc.playerMutex.Lock()
			result[fmt.Sprintf("%v", i)] = bpc.sourceTablet.String()
			bpc.playerMutex.Unlock()
		}
		blm.mu.Unlock()
		return result
	}))
}

func (blm *BinlogPlayerMap) size() int64 {
	blm.mu.Lock()
	result := len(blm.players)
	blm.mu.Unlock()
	return int64(result)
}

// addPlayer adds a new player to the map. It assumes we have the lock.
func (blm *BinlogPlayerMap) addPlayer(ctx context.Context, cell string, keyspaceIDType pb.KeyspaceIdType, keyRange *pb.KeyRange, sourceShard *pb.Shard_SourceShard, dbName string) {
	bpc, ok := blm.players[sourceShard.Uid]
	if ok {
		log.Infof("Already playing logs for %v", sourceShard)
		return
	}

	bpc = newBinlogPlayerController(blm.ts, blm.dbConfig, blm.mysqld, cell, keyspaceIDType, keyRange, sourceShard, dbName)
	blm.players[sourceShard.Uid] = bpc
	if blm.state == BpmStateRunning {
		bpc.Start(ctx)
	}
}

// StopAllPlayersAndReset stops all the binlog players, and reset the map of players.
func (blm *BinlogPlayerMap) StopAllPlayersAndReset() {
	hadPlayers := false
	blm.mu.Lock()
	for _, bpc := range blm.players {
		if blm.state == BpmStateRunning {
			bpc.Stop()
		}
		hadPlayers = true
	}
	blm.players = make(map[uint32]*BinlogPlayerController)
	blm.mu.Unlock()

	if hadPlayers {
		// We're done streaming, so turn off special playback settings.
		blm.mysqld.DisableBinlogPlayback()
	}
}

// RefreshMap reads the right data from topo.Server and makes sure
// we're playing the right logs.
func (blm *BinlogPlayerMap) RefreshMap(ctx context.Context, tablet *topo.Tablet, keyspaceInfo *topo.KeyspaceInfo, shardInfo *topo.ShardInfo) {
	log.Infof("Refreshing map of binlog players")
	if shardInfo == nil {
		log.Warningf("Could not read shardInfo, not changing anything")
		return
	}

	if len(shardInfo.SourceShards) > 0 && keyspaceInfo == nil {
		log.Warningf("Could not read keyspaceInfo, not changing anything")
		return
	}

	blm.mu.Lock()
	if blm.dbConfig.DbName == "" {
		blm.dbConfig.DbName = tablet.DbName()
	}

	// get the existing sources and build a map of sources to remove
	toRemove := make(map[uint32]bool)
	hadPlayers := false
	for source := range blm.players {
		toRemove[source] = true
		hadPlayers = true
	}

	// for each source, add it if not there, and delete from toRemove
	for _, sourceShard := range shardInfo.SourceShards {
		blm.addPlayer(ctx, tablet.Alias.Cell, keyspaceInfo.ShardingColumnType, key.KeyRangeToProto(tablet.KeyRange), sourceShard, tablet.DbName())
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
		// We're done streaming, so turn off special playback settings.
		blm.mysqld.DisableBinlogPlayback()
	}
}

// Stop stops the current players, but does not remove them from the map.
// Call 'Start' to restart the playback.
func (blm *BinlogPlayerMap) Stop() {
	blm.mu.Lock()
	defer blm.mu.Unlock()
	if blm.state == BpmStateStopped {
		log.Warningf("BinlogPlayerMap already stopped")
		return
	}
	log.Infof("Stopping map of binlog players")
	for _, bpc := range blm.players {
		bpc.Stop()
	}
	blm.state = BpmStateStopped
}

// Start restarts the current players.
func (blm *BinlogPlayerMap) Start(ctx context.Context) {
	blm.mu.Lock()
	defer blm.mu.Unlock()
	if blm.state == BpmStateRunning {
		log.Warningf("BinlogPlayerMap already started")
		return
	}
	log.Infof("Starting map of binlog players")
	for _, bpc := range blm.players {
		bpc.Start(ctx)
	}
	blm.state = BpmStateRunning
}

// BlpPositionList returns the current position of all the players.
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
		blp, _, err := bpc.BlpPosition(vtClient)
		if err != nil {
			return nil, fmt.Errorf("can't read current position for %v: %v", bpc, err)
		}

		result.Entries = append(result.Entries, *blp)
	}
	return result, nil
}

// RunUntil will run all the players until they reach the given position.
// Holds the map lock during that exercise, shouldn't take long at all.
func (blm *BinlogPlayerMap) RunUntil(ctx context.Context, blpPositionList *blproto.BlpPositionList, waitTimeout time.Duration) error {
	// lock and check state
	blm.mu.Lock()
	defer blm.mu.Unlock()
	if blm.state != BpmStateStopped {
		return fmt.Errorf("RunUntil: player not stopped: %v", blm.state)
	}
	log.Infof("Starting map of binlog players until position")

	// find the exact stop position for all players, to be sure
	// we're not doing anything wrong
	posMap := make(map[uint32]myproto.ReplicationPosition)
	for _, bpc := range blm.players {
		blpPos, err := blpPositionList.FindBlpPositionById(bpc.sourceShard.Uid)
		if err != nil {
			return fmt.Errorf("No binlog position passed in for player Uid %v", bpc.sourceShard.Uid)
		}
		posMap[bpc.sourceShard.Uid] = blpPos.Position
	}

	// start all the players giving them where to stop
	for _, bpc := range blm.players {
		if err := bpc.StartUntil(ctx, posMap[bpc.sourceShard.Uid]); err != nil {
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

// BinlogPlayerControllerStatus is the status of an individual controller.
type BinlogPlayerControllerStatus struct {
	// configuration values
	Index        uint32
	SourceShard  *pb.Shard_SourceShard
	StopPosition myproto.ReplicationPosition

	// stats and current values
	LastPosition        myproto.ReplicationPosition
	SecondsBehindMaster int64
	Counts              map[string]int64
	Rates               map[string][]float64
	State               string
	SourceTablet        topo.TabletAlias
	LastError           string
}

// SourceShardAsHTML returns the SourceShard as HTML
func (bpcs *BinlogPlayerControllerStatus) SourceShardAsHTML() template.HTML {
	return topo.SourceShardAsHTML(bpcs.SourceShard)
}

// BinlogPlayerControllerStatusList is the list of statuses.
type BinlogPlayerControllerStatusList []*BinlogPlayerControllerStatus

// Len is part of sort.Interface.
func (bpcsl BinlogPlayerControllerStatusList) Len() int {
	return len(bpcsl)
}

// Less is part of sort.Interface.
func (bpcsl BinlogPlayerControllerStatusList) Less(i, j int) bool {
	return bpcsl[i].Index < bpcsl[j].Index
}

// Swap is part of sort.Interface.
func (bpcsl BinlogPlayerControllerStatusList) Swap(i, j int) {
	bpcsl[i], bpcsl[j] = bpcsl[j], bpcsl[i]
}

// BinlogPlayerMapStatus is the complete player status.
type BinlogPlayerMapStatus struct {
	State       string
	Controllers BinlogPlayerControllerStatusList
}

// Status returns the BinlogPlayerMapStatus for the BinlogPlayerMap.
func (blm *BinlogPlayerMap) Status() *BinlogPlayerMapStatus {
	// Create the result, take care of the stopped state.
	result := &BinlogPlayerMapStatus{}
	blm.mu.Lock()
	defer blm.mu.Unlock()

	// fill in state
	if blm.state == BpmStateStopped {
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
			StopPosition:        bpc.stopPosition,
			LastPosition:        bpc.binlogPlayerStats.GetLastPosition(),
			SecondsBehindMaster: bpc.binlogPlayerStats.SecondsBehindMaster.Get(),
			Counts:              bpc.binlogPlayerStats.Timings.Counts(),
			Rates:               bpc.binlogPlayerStats.Rates.Get(),
		}
		bpc.playerMutex.Lock()
		if bpc.ctx == nil {
			bpcs.State = "Stopped"
		} else {
			bpcs.State = "Running"
		}
		bpcs.SourceTablet = bpc.sourceTablet
		if bpc.lastError != nil {
			bpcs.LastError = bpc.lastError.Error()
		}
		bpc.playerMutex.Unlock()
		result.Controllers = append(result.Controllers, bpcs)
	}
	sort.Sort(result.Controllers)

	return result
}
