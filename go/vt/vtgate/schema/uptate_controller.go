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

	"vitess.io/vitess/go/vt/discovery"
)

type (
	queue struct {
		items []*discovery.TabletHealth
	}

	updateController struct {
		mu           sync.Mutex
		queue        *queue
		consumeDelay time.Duration
		update       func(th *discovery.TabletHealth) bool
		init         func(th *discovery.TabletHealth) bool
		signal       func()
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
		if u.init != nil {
			success = u.init(item)
			if success {
				u.init = nil
			}
		} else {
			success = u.update(item)
		}
		if success && u.signal != nil {
			u.signal()
		}
	}
}

func (u *updateController) getItemFromQueueLocked() *discovery.TabletHealth {
	item := u.queue.items[0]
	i := 0
	for ; i < len(u.queue.items); i++ {
		for _, table := range u.queue.items[i].TablesUpdated {
			found := false
			for _, itemTable := range item.TablesUpdated {
				if itemTable == table {
					found = true
					break
				}
			}
			if !found {
				item.TablesUpdated = append(item.TablesUpdated, table)
			}
		}
	}
	// emptying queue's items as all items from 0 to i (length of the queue) are merged
	u.queue.items = u.queue.items[i:]
	return item
}

func (u *updateController) add(th *discovery.TabletHealth) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if len(th.TablesUpdated) == 0 && u.init == nil {
		return
	}
	if u.queue == nil {
		u.queue = &queue{}
		go u.consume()
	}
	u.queue.items = append(u.queue.items, th)
}
