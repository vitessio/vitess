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

	"vitess.io/vitess/go/vt/discovery"
)

type (
	queue struct {
		items []*discovery.TabletHealth
	}

	updateController struct {
		mu     sync.Mutex
		queue  *queue
		update func(th *discovery.TabletHealth)
		init   func(th *discovery.TabletHealth)
		signal func()
	}
)

func (u *updateController) consume() {
	for {
		u.mu.Lock()
		var item *discovery.TabletHealth

		if len(u.queue.items) == 0 {
			u.queue = nil
			u.mu.Unlock()
			return
		}
		// todo: scan queue for multiple update from the same shard, be clever
		item = u.queue.items[0]
		u.queue.items = u.queue.items[1:]
		u.mu.Unlock()

		if u.init != nil {
			u.init(item)
			u.init = nil
		} else {
			if len(item.TablesUpdated) == 0 {
				continue
			}
			u.update(item)
		}
		if u.signal != nil {
			u.signal()
		}
	}
}

func (u *updateController) add(th *discovery.TabletHealth) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.queue == nil {
		u.queue = &queue{}
		go u.consume()
	}
	u.queue.items = append(u.queue.items, th)
}
