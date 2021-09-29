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
		reloadKeyspace func(th *discovery.TabletHealth) bool
		signal         func()
		loaded         bool
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
		u.mu.Unlock()

		var success bool
		if u.loaded {
			success = u.update(item)
		} else {
			success = u.reloadKeyspace(item)
		}
		if success && u.signal != nil {
			u.signal()
		}
	}
}

func (u *updateController) getItemFromQueueLocked() *discovery.TabletHealth {
	item := u.queue.items[0]
	itemsCount := len(u.queue.items)
	// Only when we want to update selected tables.
	if u.loaded {
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
		}
	}
	// emptying queue's items as all items from 0 to i (length of the queue) are merged
	u.queue.items = u.queue.items[itemsCount:]
	return item
}

func (u *updateController) add(th *discovery.TabletHealth) {
	// For non-primary tablet health, there is no schema tracking.
	if th.Tablet.Type != topodatapb.TabletType_PRIMARY {
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
	if len(th.Stats.TableSchemaChanged) == 0 && u.loaded {
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
