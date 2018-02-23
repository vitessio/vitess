package datastore

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	log "code.google.com/p/log4go"
	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/datastore/storage"
	"github.com/influxdb/influxdb/metastore"
	"github.com/influxdb/influxdb/protocol"
)

type ShardDatastore struct {
	baseDbDir      string
	config         *configuration.Configuration
	shards         map[uint32]*Shard
	lastAccess     map[uint32]time.Time
	shardRefCounts map[uint32]int
	shardsToClose  map[uint32]bool
	shardsToDelete map[uint32]struct{}
	shardsLock     sync.RWMutex
	writeBuffer    *cluster.WriteBuffer
	maxOpenShards  int
	pointBatchSize int
	writeBatchSize int
	metaStore      *metastore.Store
}

const (
	ONE_KILOBYTE                    = 1024
	ONE_MEGABYTE                    = 1024 * 1024
	SHARD_BLOOM_FILTER_BITS_PER_KEY = 10
	SHARD_DATABASE_DIR              = "shard_db_v2"
)

var (

	// This datastore implements the PersistentAtomicInteger interface. All of the persistent
	// integers start with this prefix, followed by their name
	ATOMIC_INCREMENT_PREFIX = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD}
	// NEXT_ID_KEY holds the next id. ids are used to "intern" timeseries and column names
	NEXT_ID_KEY = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	// SERIES_COLUMN_INDEX_PREFIX is the prefix of the series to column names index
	SERIES_COLUMN_INDEX_PREFIX = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}
	// DATABASE_SERIES_INDEX_PREFIX is the prefix of the database to series names index
	DATABASE_SERIES_INDEX_PREFIX = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	MAX_SEQUENCE                 = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	// replicateWrite = protocol.Request_REPLICATION_WRITE

	TRUE = true
)

func NewShardDatastore(config *configuration.Configuration, metaStore *metastore.Store) (*ShardDatastore, error) {
	baseDbDir := filepath.Join(config.DataDir, SHARD_DATABASE_DIR)
	err := os.MkdirAll(baseDbDir, 0744)
	if err != nil {
		return nil, err
	}

	return &ShardDatastore{
		baseDbDir:      baseDbDir,
		config:         config,
		shards:         make(map[uint32]*Shard),
		maxOpenShards:  config.StorageMaxOpenShards,
		lastAccess:     make(map[uint32]time.Time),
		shardRefCounts: make(map[uint32]int),
		shardsToClose:  make(map[uint32]bool),
		shardsToDelete: make(map[uint32]struct{}),
		pointBatchSize: config.StoragePointBatchSize,
		writeBatchSize: config.StorageWriteBatchSize,
		metaStore:      metaStore,
	}, nil
}

func (self *ShardDatastore) Close() {
	self.shardsLock.Lock()
	defer self.shardsLock.Unlock()
	for _, shard := range self.shards {
		shard.close()
	}
}

// Get the engine that was used when the shard was created if it
// exists or set the type of the default engine type
func (self *ShardDatastore) getEngine(dir string) (string, error) {
	shardExist := true
	info, err := os.Stat(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", err
		}

		shardExist = false
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", err
		}
	} else if !info.IsDir() {
		return "", fmt.Errorf("%s isn't a directory", dir)
	}

	f, err := os.OpenFile(path.Join(dir, "type"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()

	body, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}

	if s := string(body); s != "" {
		return s, nil
	}

	// If the shard existed already but the 'type' file didn't, assume
	// it's leveldb
	t := self.config.StorageDefaultEngine
	if shardExist {
		t = "leveldb"
	}

	if _, err := f.WriteString(t); err != nil {
		return "", err
	}

	if err := f.Sync(); err != nil {
		return "", err
	}

	return t, nil
}

func (self *ShardDatastore) GetOrCreateShard(id uint32) (cluster.LocalShardDb, error) {
	now := time.Now()
	self.shardsLock.Lock()
	defer self.shardsLock.Unlock()
	db := self.shards[id]
	self.lastAccess[id] = now

	if db != nil {
		self.incrementShardRefCountAndCloseOldestIfNeeded(id)
		return db, nil
	}

	dbDir := self.shardDir(id)

	log.Info("DATASTORE: opening or creating shard %s", dbDir)
	engine, err := self.getEngine(dbDir)
	if err != nil {
		return nil, err
	}
	init, err := storage.GetInitializer(engine)
	if err != nil {
		log.Error("Error opening shard: ", err)
		return nil, err
	}
	c := init.NewConfig()
	conf, ok := self.config.StorageEngineConfigs[engine]
	if err := toml.PrimitiveDecode(conf, c); ok && err != nil {
		return nil, err
	}

	// TODO: this is for backward compatability with the old
	// configuration
	if leveldbConfig, ok := c.(*storage.LevelDbConfiguration); ok {
		if leveldbConfig.LruCacheSize == 0 {
			leveldbConfig.LruCacheSize = configuration.Size(self.config.LevelDbLruCacheSize)
		}

		if leveldbConfig.MaxOpenFiles == 0 {
			leveldbConfig.MaxOpenFiles = self.config.LevelDbMaxOpenFiles
		}
	}

	se, err := init.Initialize(dbDir, c)
	if err != nil {
		os.RemoveAll(dbDir)
		return nil, err
	}

	db, err = NewShard(se, self.pointBatchSize, self.writeBatchSize, self.metaStore)
	if err != nil {
		log.Error("Error creating shard: ", err)
		se.Close()
		return nil, err
	}
	self.shards[id] = db
	self.incrementShardRefCountAndCloseOldestIfNeeded(id)
	return db, nil
}

func (self *ShardDatastore) incrementShardRefCountAndCloseOldestIfNeeded(id uint32) {
	self.shardRefCounts[id] += 1
	delete(self.shardsToClose, id)
	if self.maxOpenShards > 0 && len(self.shards) > self.maxOpenShards {
		for i := len(self.shards) - self.maxOpenShards; i > 0; i-- {
			self.closeOldestShard()
		}
	}
}

func (self *ShardDatastore) ReturnShard(id uint32) {
	self.shardsLock.Lock()
	defer self.shardsLock.Unlock()
	log.Fine("Returning shard. id = %d", id)
	self.shardRefCounts[id] -= 1
	if self.shardRefCounts[id] != 0 {
		return
	}

	log.Fine("Checking if shard should be deleted. id = %d", id)
	if _, ok := self.shardsToDelete[id]; ok {
		self.deleteShard(id)
		return
	}

	log.Fine("Checking if shard should be closed. id = %d", id)
	if self.shardsToClose[id] {
		self.closeShard(id)
	}
}

func (self *ShardDatastore) Write(request *protocol.Request) error {
	shardDb, err := self.GetOrCreateShard(*request.ShardId)
	if err != nil {
		return err
	}
	defer self.ReturnShard(*request.ShardId)
	return shardDb.Write(*request.Database, request.MultiSeries)
}

func (self *ShardDatastore) BufferWrite(request *protocol.Request) {
	self.writeBuffer.Write(request)
}

func (self *ShardDatastore) SetWriteBuffer(writeBuffer *cluster.WriteBuffer) {
	self.writeBuffer = writeBuffer
}

func (self *ShardDatastore) DeleteShard(shardId uint32) {
	self.shardsLock.Lock()
	defer self.shardsLock.Unlock()
	// If someone has a reference to the shard we can't delete it
	// now. We have to wait until it's returned and delete
	// it. ReturnShard will take care of that as soon as the reference
	// count becomes 0.
	if refc := self.shardRefCounts[shardId]; refc > 0 {
		log.Fine("Cannot delete shard: shardId = %d, shardRefCounts[shardId] = %d", shardId, refc)
		self.shardsToDelete[shardId] = struct{}{}
		return
	}

	// otherwise, close the shard and delete it now
	self.deleteShard(shardId)
}

func (self *ShardDatastore) shardDir(id uint32) string {
	return filepath.Join(self.baseDbDir, fmt.Sprintf("%.5d", id))
}

func (self *ShardDatastore) closeOldestShard() {
	var oldestId uint32
	oldestAccess := time.Now()
	for id, lastAccess := range self.lastAccess {
		if lastAccess.Before(oldestAccess) && self.shardsToClose[id] == false {
			oldestId = id
			oldestAccess = lastAccess
		}
	}
	if self.shardRefCounts[oldestId] == 0 {
		self.closeShard(oldestId)
	} else {
		self.shardsToClose[oldestId] = true
	}
}

func (self *ShardDatastore) deleteShard(id uint32) {
	self.closeShard(id)
	dir := self.shardDir(id)
	log.Info("DATASTORE: dropping shard %s", dir)
	if err := os.RemoveAll(dir); err != nil {
		// TODO: we should do some cleanup to make sure any shards left
		// behind are deleted properly
		log.Error("Cannot delete %s: %s", dir, err)
	}
}

func (self *ShardDatastore) closeShard(id uint32) {
	shard := self.shards[id]
	if shard != nil {
		shard.close()
	}
	delete(self.shardRefCounts, id)
	delete(self.shards, id)
	delete(self.lastAccess, id)
	delete(self.shardsToClose, id)
	delete(self.shardsToDelete, id)
	log.Debug("DATASTORE: closing shard %s", self.shardDir(id))
}

// // returns true if the point has the correct field id and is
// // in the given time range
func isPointInRange(fieldId, startTime, endTime, point []byte) bool {
	id := point[:8]
	time := point[8:16]
	return bytes.Equal(id, fieldId) && bytes.Compare(time, startTime) > -1 && bytes.Compare(time, endTime) < 1
}

type FieldLookupError struct {
	message string
}

func NewFieldLookupError(message string) *FieldLookupError {
	return &FieldLookupError{message}
}

func (self FieldLookupError) Error() string {
	return self.message
}
