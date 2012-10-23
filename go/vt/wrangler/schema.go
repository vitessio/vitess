package zkwrangler

import (
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"code.google.com/p/vitess/go/vt/mysqlctl"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

func (wr *Wrangler) GetSchema(zkTabletPath string) (*mysqlctl.SchemaDefinition, error) {
	ti, err := tm.ReadTablet(wr.zconn, zkTabletPath)
	if err != nil {
		return nil, err
	}
	zkReplyPath := path.Join(tm.TabletActionPath(zkTabletPath), path.Base(ti.Path())+"_get_schema_reply.json")
	actionPath, err := wr.ai.GetSchema(ti.Path(), zkReplyPath)
	if err != nil {
		return nil, err
	}
	err = wr.ai.WaitForCompletion(actionPath, 1*time.Minute)
	if err != nil {
		return nil, err
	}
	data, _, err := wr.zconn.Get(zkReplyPath)
	if err != nil {
		return nil, err
	}
	sd := new(mysqlctl.SchemaDefinition)
	if err = json.Unmarshal([]byte(data), sd); err != nil {
		return nil, err
	}
	return sd, nil
}

// helper method to asynchronously diff a schema
func (wr *Wrangler) diffSchema(masterSchema *mysqlctl.SchemaDefinition, zkMasterTabletPath string, alias tm.TabletAlias, wg *sync.WaitGroup, result chan string) {
	defer wg.Done()
	zkTabletPath := tm.TabletPathForAlias(alias)
	slaveSchema, err := wr.GetSchema(zkTabletPath)
	if err != nil {
		result <- err.Error()
		return
	}

	masterSchema.DiffSchema(zkMasterTabletPath, zkTabletPath, slaveSchema, result)
}

func channelToError(stream chan string) error {
	result := ""
	for text := range stream {
		if result != "" {
			result += "\n"
		}
		result += text
	}
	if result == "" {
		return nil
	}

	return fmt.Errorf("Schema diffs:\n%v", result)
}

func (wr *Wrangler) ValidateSchemaShard(zkShardPath string) error {
	si, err := tm.ReadShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	// get schema from the master, or error
	if si.MasterAlias.Uid == 0 {
		return fmt.Errorf("No master in shard " + zkShardPath)
	}
	zkMasterTabletPath := tm.TabletPathForAlias(si.MasterAlias)
	masterSchema, err := wr.GetSchema(zkMasterTabletPath)
	if err != nil {
		return err
	}

	// then diff with all slaves
	result := make(chan string, 10)
	wg := &sync.WaitGroup{}
	for _, alias := range si.ReplicaAliases {
		wg.Add(1)
		go wr.diffSchema(masterSchema, zkMasterTabletPath, alias, wg, result)
	}
	for _, alias := range si.RdonlyAliases {
		wg.Add(1)
		go wr.diffSchema(masterSchema, zkMasterTabletPath, alias, wg, result)
	}

	wg.Wait()
	close(result)
	return channelToError(result)
}

func (wr *Wrangler) ValidateSchemaKeyspace(zkKeyspacePath string) error {
	// find all the shards
	zkShardsPath := path.Join(zkKeyspacePath, "shards")
	shards, _, err := wr.zconn.Children(zkShardsPath)
	if err != nil {
		return err
	}

	// corner cases
	if len(shards) == 0 {
		return fmt.Errorf("No shards in keyspace " + zkKeyspacePath)
	}
	referenceShardPath := path.Join(zkShardsPath, shards[0])
	if len(shards) == 1 {
		return wr.ValidateSchemaShard(referenceShardPath)
	}

	// find the reference schema using the first shard's master
	si, err := tm.ReadShard(wr.zconn, referenceShardPath)
	if err != nil {
		return err
	}
	if si.MasterAlias.Uid == 0 {
		return fmt.Errorf("No master in shard " + referenceShardPath)
	}
	zkReferenceTabletPath := tm.TabletPathForAlias(si.MasterAlias)
	referenceSchema, err := wr.GetSchema(zkReferenceTabletPath)
	if err != nil {
		return err
	}

	//
	// then diff with all slaves
	result := make(chan string, 10)
	wg := &sync.WaitGroup{}

	// first diff the slaves in the main shard
	for _, alias := range si.ReplicaAliases {
		wg.Add(1)
		go wr.diffSchema(referenceSchema, zkReferenceTabletPath, alias, wg, result)
	}
	for _, alias := range si.RdonlyAliases {
		wg.Add(1)
		go wr.diffSchema(referenceSchema, zkReferenceTabletPath, alias, wg, result)
	}

	// then diffs the masters in the other shards, along with
	// their slaves
	for _, shard := range shards[1:] {
		shardPath := path.Join(zkShardsPath, shard)
		si, err := tm.ReadShard(wr.zconn, shardPath)
		if err != nil {
			result <- err.Error()
			continue
		}

		if si.MasterAlias.Uid == 0 {
			result <- "No master in shard " + shardPath
			continue
		}

		wg.Add(1)
		go wr.diffSchema(referenceSchema, zkReferenceTabletPath, si.MasterAlias, wg, result)
		for _, alias := range si.ReplicaAliases {
			wg.Add(1)
			go wr.diffSchema(referenceSchema, zkReferenceTabletPath, alias, wg, result)
		}
		for _, alias := range si.RdonlyAliases {
			wg.Add(1)
			go wr.diffSchema(referenceSchema, zkReferenceTabletPath, alias, wg, result)
		}
	}

	wg.Wait()
	close(result)
	return channelToError(result)
}
