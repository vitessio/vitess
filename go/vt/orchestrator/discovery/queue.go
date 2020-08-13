/*
   Copyright 2014 Outbrain Inc.

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

/*

package discovery manages a queue of discovery requests: an ordered
queue with no duplicates.

push() operation never blocks while pop() blocks on an empty queue.

*/

package discovery

import (
	"sync"
	"time"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/inst"
)

// QueueMetric contains the queue's active and queued sizes
type QueueMetric struct {
	Active int
	Queued int
}

// Queue contains information for managing discovery requests
type Queue struct {
	sync.Mutex

	name         string
	done         chan struct{}
	queue        chan inst.InstanceKey
	queuedKeys   map[inst.InstanceKey]time.Time
	consumedKeys map[inst.InstanceKey]time.Time
	metrics      []QueueMetric
}

// DiscoveryQueue contains the discovery queue which can then be accessed via an API call for monitoring.
// Currently this is accessed by ContinuousDiscovery() but also from http api calls.
// I may need to protect this better?
var discoveryQueue map[string](*Queue)
var dcLock sync.Mutex

func init() {
	discoveryQueue = make(map[string](*Queue))
}

// StopMonitoring stops monitoring all the queues
func StopMonitoring() {
	for _, q := range discoveryQueue {
		q.stopMonitoring()
	}
}

// CreateOrReturnQueue allows for creation of a new discovery queue or
// returning a pointer to an existing one given the name.
func CreateOrReturnQueue(name string) *Queue {
	dcLock.Lock()
	defer dcLock.Unlock()
	if q, found := discoveryQueue[name]; found {
		return q
	}

	q := &Queue{
		name:         name,
		queuedKeys:   make(map[inst.InstanceKey]time.Time),
		consumedKeys: make(map[inst.InstanceKey]time.Time),
		queue:        make(chan inst.InstanceKey, config.Config.DiscoveryQueueCapacity),
	}
	go q.startMonitoring()

	discoveryQueue[name] = q

	return q
}

// monitoring queue sizes until we are told to stop
func (q *Queue) startMonitoring() {
	log.Debugf("Queue.startMonitoring(%s)", q.name)
	ticker := time.NewTicker(time.Second) // hard-coded at every second

	for {
		select {
		case <-ticker.C: // do the periodic expiry
			q.collectStatistics()
		case <-q.done:
			return
		}
	}
}

// Stop monitoring the queue
func (q *Queue) stopMonitoring() {
	q.done <- struct{}{}
}

// do a check of the entries in the queue, both those active and queued
func (q *Queue) collectStatistics() {
	q.Lock()
	defer q.Unlock()

	q.metrics = append(q.metrics, QueueMetric{Queued: len(q.queuedKeys), Active: len(q.consumedKeys)})

	// remove old entries if we get too big
	if len(q.metrics) > config.Config.DiscoveryQueueMaxStatisticsSize {
		q.metrics = q.metrics[len(q.metrics)-config.Config.DiscoveryQueueMaxStatisticsSize:]
	}
}

// QueueLen returns the length of the queue (channel size + queued size)
func (q *Queue) QueueLen() int {
	q.Lock()
	defer q.Unlock()

	return len(q.queue) + len(q.queuedKeys)
}

// Push enqueues a key if it is not on a queue and is not being
// processed; silently returns otherwise.
func (q *Queue) Push(key inst.InstanceKey) {
	q.Lock()
	defer q.Unlock()

	// is it enqueued already?
	if _, found := q.queuedKeys[key]; found {
		return
	}

	// is it being processed now?
	if _, found := q.consumedKeys[key]; found {
		return
	}

	q.queuedKeys[key] = time.Now()
	q.queue <- key
}

// Consume fetches a key to process; blocks if queue is empty.
// Release must be called once after Consume.
func (q *Queue) Consume() inst.InstanceKey {
	q.Lock()
	queue := q.queue
	q.Unlock()

	key := <-queue

	q.Lock()
	defer q.Unlock()

	// alarm if have been waiting for too long
	timeOnQueue := time.Since(q.queuedKeys[key])
	if timeOnQueue > time.Duration(config.Config.InstancePollSeconds)*time.Second {
		log.Warningf("key %v spent %.4fs waiting on a discoveryQueue", key, timeOnQueue.Seconds())
	}

	q.consumedKeys[key] = q.queuedKeys[key]

	delete(q.queuedKeys, key)

	return key
}

// Release removes a key from a list of being processed keys
// which allows that key to be pushed into the queue again.
func (q *Queue) Release(key inst.InstanceKey) {
	q.Lock()
	defer q.Unlock()

	delete(q.consumedKeys, key)
}
