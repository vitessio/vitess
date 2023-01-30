/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schema

import (
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/vt/discovery"
)

type (
	queue struct {
		items []*discovery.TabletHealth
	}

	updateController struct {
		mu             sync.Mutex
		queue          *queue
		consumeDelay   time.Duration
		update         func(th *discovery.TabletHealth) bool
		reloadKeyspace func(th *discovery.TabletHealth) error
		signal         func()
		loaded         bool

		// we'll only log a failed keyspace loading once
		ignore bool
	}
)

func (u *updateController) consume() {
	for {
		time.Sleep(u.consumeDelay)

		u.mu.Lock()
		if len(u.queue.items) == 0 {
			u.queue = nil
			u.mu.Unlock()
			return
		}

		// todo: scan queue for multiple update from the same shard, be clever
		item := u.getItemFromQueueLocked()
		loaded := u.loaded
		u.mu.Unlock()

		var success bool
		if loaded {
			success = u.update(item)
		} else {
			if err := u.reloadKeyspace(item); err == nil {
				success = true
			} else {
				if checkIfWeShouldIgnoreKeyspace(err) {
					u.setIgnore(true)
				}
				success = false
			}
		}
		if success && u.signal != nil {
			u.signal()
		}
	}
}

// checkIfWeShouldIgnoreKeyspace inspects an error and
// will mark a keyspace as failed and won't try to load more information from it
func checkIfWeShouldIgnoreKeyspace(err error) bool {
	sqlErr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
	if sqlErr.Num == mysql.ERBadDb || sqlErr.Num == mysql.ERNoSuchTable {
		// if we are missing the db or table, no point in retrying
		return true
	}
	return false
}

func (u *updateController) getItemFromQueueLocked() *discovery.TabletHealth {
	item := u.queue.items[0]
	itemsCount := len(u.queue.items)
	// Only when we want to update selected tables.
	if u.loaded {
		// We are trying to minimize the vttablet calls here by merging all the table/view changes received into a single changed item
		// with all the table and view names.
		for i := 1; i < itemsCount; i++ {
			for _, table := range u.queue.items[i].Stats.TableSchemaChanged {
				found := false
				for _, itemTable := range item.Stats.TableSchemaChanged {
					if itemTable == table {
						found = true
						break
					}
				}
				if !found {
					item.Stats.TableSchemaChanged = append(item.Stats.TableSchemaChanged, table)
				}
			}
			for _, view := range u.queue.items[i].Stats.ViewSchemaChanged {
				found := false
				for _, itemView := range item.Stats.ViewSchemaChanged {
					if itemView == view {
						found = true
						break
					}
				}
				if !found {
					item.Stats.ViewSchemaChanged = append(item.Stats.ViewSchemaChanged, view)
				}
			}
		}
	}
	// emptying queue's items as all items from 0 to i (length of the queue) are merged
	u.queue.items = u.queue.items[itemsCount:]
	return item
}

func (u *updateController) add(th *discovery.TabletHealth) {
	// For non-primary tablet health, there is no schema tracking.
	if th.Target.TabletType != topodatapb.TabletType_PRIMARY {
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	// Received a health check from primary tablet that is not reachable from VTGate.
	// The connection will get reset and the tracker needs to reload the schema for the keyspace.
	if !th.Serving {
		u.loaded = false
		return
	}

	// If the keyspace schema is loaded and there is no schema change detected. Then there is nothing to process.
	if len(th.Stats.TableSchemaChanged) == 0 && len(th.Stats.ViewSchemaChanged) == 0 && u.loaded {
		return
	}

	if (len(th.Stats.TableSchemaChanged) > 0 || len(th.Stats.ViewSchemaChanged) > 0) && u.ignore {
		// we got an update for this keyspace - we need to stop ignoring it, and reload everything
		u.ignore = false
		u.loaded = false
	}

	if u.ignore {
		// keyspace marked as not working correctly, so we are ignoring it for now
		return
	}

	if u.queue == nil {
		u.queue = &queue{}
		go u.consume()
	}
	u.queue.items = append(u.queue.items, th)
}

func (u *updateController) setLoaded(loaded bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.loaded = loaded
}

func (u *updateController) setIgnore(i bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.ignore = i
}
