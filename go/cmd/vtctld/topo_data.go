package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
)

// This file includes the support for serving topo data to an ajax-based
// front-end. There are three ways we collect data:
// - reading topology records that don't change too often, caching them
//   with a somewhat big TTL, and have a 'flush' command in case it's needed.
//   (list of cells, tablets in a cell/shard, ...)
// - subscribing to topology change channels (serving graph)
// - establishing streaming connections to vttablets to get up-to-date
//   health reports.

// VersionedObject is the interface implemented by objects in the versioned
// object cache VersionedObjectCache object.
type VersionedObject interface {
	GetVersion() int
	SetVersion(int)
	Reset()
}

// BaseVersionedObject is the base implementation for VersionedObject
// that handles the version stuff. It handles GetVersion and SetVersion.
// Inherited types still need to implement Reset().
type BaseVersionedObject struct {
	Version int
}

// GetVersion is part of the VersionedObject interface
func (bvo *BaseVersionedObject) GetVersion() int {
	return bvo.Version
}

// SetVersion is part of the VersionedObject interface
func (bvo *BaseVersionedObject) SetVersion(version int) {
	bvo.Version = version
}

// GetVersionedObjectFunc is the function the cache will call to get
// the object itself.
type GetVersionedObjectFunc func() (VersionedObject, error)

// VersionedObjectCache is the main cache object. Just needs a method to get
// the content.
type VersionedObjectCache struct {
	getObject GetVersionedObjectFunc

	mu              sync.Mutex
	timestamp       time.Time
	versionedObject VersionedObject
	result          []byte
}

// NewVersionedObjectCache returns a new VersionedObjectCache object.
func NewVersionedObjectCache(getObject GetVersionedObjectFunc) *VersionedObjectCache {
	return &VersionedObjectCache{
		getObject: getObject,
	}
}

// Get returns the versioned value from the cache.
func (voc *VersionedObjectCache) Get() ([]byte, error) {
	voc.mu.Lock()
	defer voc.mu.Unlock()

	now := time.Now()
	if now.Sub(voc.timestamp) < 5*time.Minute {
		return voc.result, nil
	}

	newObject, err := voc.getObject()
	if err != nil {
		return nil, err
	}
	if voc.versionedObject != nil {
		// we already have an object, check if it's equal
		newObject.SetVersion(voc.versionedObject.GetVersion())
		if reflect.DeepEqual(newObject, voc.versionedObject) {
			voc.timestamp = now
			return voc.result, nil
		}

		// it's not equal increment the version
		newObject.SetVersion(voc.versionedObject.GetVersion() + 1)
	} else {
		newObject.SetVersion(1)
	}

	// we have a new object, update the version, and save it
	voc.result, err = json.MarshalIndent(newObject, "", "  ")
	if err != nil {
		return nil, err
	}
	voc.versionedObject = newObject
	voc.timestamp = now
	return voc.result, nil
}

// Flush will flush the cache, and force a version increment, so
// clients will reload.
func (voc *VersionedObjectCache) Flush() {
	voc.mu.Lock()
	defer voc.mu.Unlock()

	// we reset timestamp and content, so the Version will increase again
	// and force a client refresh, even if the data is the same.
	voc.timestamp = time.Time{}
	if voc.versionedObject != nil {
		voc.versionedObject.Reset()
	}
}

// VersionedObjectCacheFactory knows how to construct a VersionedObjectCache
// from the key
type VersionedObjectCacheFactory func(key string) *VersionedObjectCache

// VersionedObjectCacheMap is a map of VersionedObjectCache protected
// by a mutex.
type VersionedObjectCacheMap struct {
	factory VersionedObjectCacheFactory

	mu       sync.Mutex
	cacheMap map[string]*VersionedObjectCache
}

// NewVersionedObjectCacheMap returns a new VersionedObjectCacheFactory
// that uses the factory method to create individual VersionedObjectCache.
func NewVersionedObjectCacheMap(factory VersionedObjectCacheFactory) *VersionedObjectCacheMap {
	return &VersionedObjectCacheMap{
		factory:  factory,
		cacheMap: make(map[string]*VersionedObjectCache),
	}
}

// Get finds the right VersionedObjectCache and returns its value
func (vocm *VersionedObjectCacheMap) Get(key string) ([]byte, error) {
	vocm.mu.Lock()
	voc, ok := vocm.cacheMap[key]
	if !ok {
		voc = vocm.factory(key)
		vocm.cacheMap[key] = voc
	}
	vocm.mu.Unlock()

	return voc.Get()
}

// Flush will flush the entire cache
func (vocm *VersionedObjectCacheMap) Flush() {
	vocm.mu.Lock()
	defer vocm.mu.Unlock()
	for _, voc := range vocm.cacheMap {
		voc.Flush()
	}
}

// KnownCells contains the cached result of topo.Server.GetKnownCells
type KnownCells struct {
	BaseVersionedObject

	// Cells is the list of Known Cells for this topology
	Cells []string
}

// Reset is part of the VersionedObject interface
func (kc *KnownCells) Reset() {
	kc.Cells = nil
}

func newKnownCellsCache(ts topo.Server) *VersionedObjectCache {
	return NewVersionedObjectCache(func() (VersionedObject, error) {
		cells, err := ts.GetKnownCells()
		if err != nil {
			return nil, err
		}
		return &KnownCells{
			Cells: cells,
		}, nil
	})
}

// Keyspaces contains the cached result of topo.Server.GetKeyspaces
type Keyspaces struct {
	BaseVersionedObject

	// Keyspaces is the list of Keyspaces for this topology
	Keyspaces []string
}

// Reset is part of the VersionedObject interface
func (k *Keyspaces) Reset() {
	k.Keyspaces = nil
}

func newKeyspacesCache(ts topo.Server) *VersionedObjectCache {
	return NewVersionedObjectCache(func() (VersionedObject, error) {
		keyspaces, err := ts.GetKeyspaces()
		if err != nil {
			return nil, err
		}
		return &Keyspaces{
			Keyspaces: keyspaces,
		}, nil
	})
}

// Keyspace contains the cached results of topo.Server.GetKeyspace
type Keyspace struct {
	BaseVersionedObject

	// KeyspaceName is the name of this keyspace
	KeyspaceName string

	// Keyspace is the topo value of this keyspace
	Keyspace *topo.Keyspace
}

// Reset is part of the VersionedObject interface
func (k *Keyspace) Reset() {
	k.KeyspaceName = ""
	k.Keyspace = nil
}

func newKeyspaceCache(ts topo.Server) *VersionedObjectCacheMap {
	return NewVersionedObjectCacheMap(func(key string) *VersionedObjectCache {
		return NewVersionedObjectCache(func() (VersionedObject, error) {
			k, err := ts.GetKeyspace(key)
			if err != nil {
				return nil, err
			}
			return &Keyspace{
				KeyspaceName: k.KeyspaceName(),
				Keyspace:     k.Keyspace,
			}, nil
		})
	})
}

// ShardNames contains the cached results of topo.Server.GetShardNames
type ShardNames struct {
	BaseVersionedObject

	// KeyspaceName is the name of the keyspace this result applies to
	KeyspaceName string

	// ShardNames is the list of shard names for this keyspace
	ShardNames []string
}

// Reset is part of the VersionedObject interface
func (s *ShardNames) Reset() {
	s.KeyspaceName = ""
	s.ShardNames = nil
}

func newShardNamesCache(ts topo.Server) *VersionedObjectCacheMap {
	return NewVersionedObjectCacheMap(func(key string) *VersionedObjectCache {
		return NewVersionedObjectCache(func() (VersionedObject, error) {
			sn, err := ts.GetShardNames(key)
			if err != nil {
				return nil, err
			}
			return &ShardNames{
				KeyspaceName: key,
				ShardNames:   sn,
			}, nil
		})
	})
}

// Shard contains the cached results of topo.Server.GetShard
// the map key is keyspace/shard
type Shard struct {
	BaseVersionedObject

	// KeyspaceName is the keyspace for this shard
	KeyspaceName string

	// ShardName is the name for this shard
	ShardName string

	// Shard is the topo value of this shard
	Shard *topo.Shard
}

// Reset is part of the VersionedObject interface
func (s *Shard) Reset() {
	s.KeyspaceName = ""
	s.ShardName = ""
	s.Shard = nil
}

func newShardCache(ts topo.Server) *VersionedObjectCacheMap {
	return NewVersionedObjectCacheMap(func(key string) *VersionedObjectCache {
		return NewVersionedObjectCache(func() (VersionedObject, error) {

			keyspace, shard, err := topo.ParseKeyspaceShardString(key)
			if err != nil {
				return nil, err
			}
			s, err := ts.GetShard(keyspace, shard)
			if err != nil {
				return nil, err
			}
			return &Shard{
				KeyspaceName: s.Keyspace(),
				ShardName:    s.ShardName(),
				Shard:        s.Shard,
			}, nil
		})
	})
}

// CellShardTablets contains the cached results of the list of tablets
// in a cell / keyspace / shard.
// The map key is cell/keyspace/shard.
type CellShardTablets struct {
	BaseVersionedObject

	// Cell is the name of the cell
	Cell string

	// KeyspaceName is the keyspace for this shard
	KeyspaceName string

	// ShardName is the name for this shard
	ShardName string

	// TabletAliases is the array os tablet aliases
	TabletAliases []topo.TabletAlias
}

// Reset is part of the VersionedObject interface
func (cst *CellShardTablets) Reset() {
	cst.Cell = ""
	cst.KeyspaceName = ""
	cst.ShardName = ""
	cst.TabletAliases = nil
}

func newCellShardTabletsCache(ts topo.Server) *VersionedObjectCacheMap {
	return NewVersionedObjectCacheMap(func(key string) *VersionedObjectCache {
		return NewVersionedObjectCache(func() (VersionedObject, error) {
			parts := strings.Split(key, "/")
			if len(parts) != 3 {
				return nil, fmt.Errorf("Invalid shard tablets path: %v", key)
			}
			sr, err := ts.GetShardReplication(parts[0], parts[1], parts[2])
			if err != nil {
				return nil, err
			}
			result := &CellShardTablets{
				Cell:          parts[0],
				KeyspaceName:  parts[1],
				ShardName:     parts[2],
				TabletAliases: make([]topo.TabletAlias, len(sr.ReplicationLinks)),
			}
			for i, rl := range sr.ReplicationLinks {
				result.TabletAliases[i] = rl.TabletAlias
			}
			return result, nil
		})
	})
}
