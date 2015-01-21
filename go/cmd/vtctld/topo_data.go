package main

import (
	"encoding/json"
	"reflect"
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
