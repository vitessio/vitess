/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package kv

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/vt/orchestrator/config"

	consulapi "github.com/armon/consul-api"
	"github.com/patrickmn/go-cache"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

// A Consul store based on config's `ConsulAddress`, `ConsulScheme`, and `ConsulKVPrefix`
type consulStore struct {
	client              *consulapi.Client
	kvCache             *cache.Cache
	distributionReentry int64
}

// NewConsulStore creates a new consul store. It is possible that the client for this store is nil,
// which is the case if no consul config is provided.
func NewConsulStore() KVStore {
	store := &consulStore{
		kvCache: cache.New(cache.NoExpiration, cache.DefaultExpiration),
	}

	if config.Config.ConsulAddress != "" {
		consulConfig := consulapi.DefaultConfig()
		consulConfig.Address = config.Config.ConsulAddress
		consulConfig.Scheme = config.Config.ConsulScheme
		if config.Config.ConsulScheme == "https" {
			consulConfig.HttpClient = &http.Client{
				Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
			}
		}
		// ConsulAclToken defaults to ""
		consulConfig.Token = config.Config.ConsulAclToken
		if client, err := consulapi.NewClient(consulConfig); err != nil {
			log.Errore(err)
		} else {
			store.client = client
		}
	}
	return store
}

func (this *consulStore) PutKeyValue(key string, value string) (err error) {
	if this.client == nil {
		return nil
	}
	pair := &consulapi.KVPair{Key: key, Value: []byte(value)}
	_, err = this.client.KV().Put(pair, nil)
	return err
}

func (this *consulStore) GetKeyValue(key string) (value string, found bool, err error) {
	if this.client == nil {
		return value, found, nil
	}
	pair, _, err := this.client.KV().Get(key, nil)
	if err != nil {
		return value, found, err
	}
	if pair == nil {
		return "", false, err
	}
	return string(pair.Value), true, nil
}

func (this *consulStore) DistributePairs(kvPairs [](*KVPair)) (err error) {
	// This function is non re-entrant (it can only be running once at any point in time)
	if atomic.CompareAndSwapInt64(&this.distributionReentry, 0, 1) {
		defer atomic.StoreInt64(&this.distributionReentry, 0)
	} else {
		return
	}

	if !config.Config.ConsulCrossDataCenterDistribution {
		return nil
	}

	datacenters, err := this.client.Catalog().Datacenters()
	if err != nil {
		return err
	}
	log.Debugf("consulStore.DistributePairs(): distributing %d pairs to %d datacenters", len(kvPairs), len(datacenters))
	consulPairs := [](*consulapi.KVPair){}
	for _, kvPair := range kvPairs {
		consulPairs = append(consulPairs, &consulapi.KVPair{Key: kvPair.Key, Value: []byte(kvPair.Value)})
	}
	var wg sync.WaitGroup
	for _, datacenter := range datacenters {
		datacenter := datacenter
		wg.Add(1)
		go func() {
			defer wg.Done()

			writeOptions := &consulapi.WriteOptions{Datacenter: datacenter}
			queryOptions := &consulapi.QueryOptions{Datacenter: datacenter}
			skipped := 0
			existing := 0
			written := 0
			failed := 0

			for _, consulPair := range consulPairs {
				val := string(consulPair.Value)
				kcCacheKey := fmt.Sprintf("%s;%s", datacenter, consulPair.Key)

				if value, found := this.kvCache.Get(kcCacheKey); found && val == value {
					skipped++
					continue
				}
				if pair, _, err := this.client.KV().Get(consulPair.Key, queryOptions); err == nil && pair != nil {
					if val == string(pair.Value) {
						existing++
						this.kvCache.SetDefault(kcCacheKey, val)
						continue
					}
				}

				if _, e := this.client.KV().Put(consulPair, writeOptions); e != nil {
					log.Errorf("consulStore.DistributePairs(): failed %s", kcCacheKey)
					failed++
					err = e
				} else {
					log.Debugf("consulStore.DistributePairs(): written %s=%s", kcCacheKey, val)
					written++
					this.kvCache.SetDefault(kcCacheKey, val)
				}
			}
			log.Debugf("consulStore.DistributePairs(): datacenter: %s; skipped: %d, existing: %d, written: %d, failed: %d", datacenter, skipped, existing, written, failed)
		}()
	}
	wg.Wait()
	return err
}
