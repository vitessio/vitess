import { Http } from '@angular/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Rx';

import { ShardService } from './shard.service';
import { Keyspace } from './keyspace';
import { Proto } from '../shared/proto';

@Injectable()
export class KeyspaceService {
  private keyspacesUrl = '../api/keyspaces/';
  private srvKeyspaceUrl = '../api/srv_keyspace/local/';

  constructor(private http: Http,
              private shardService: ShardService) {}

  getShards(keyspaceName: string): Observable<any> {
    return this.shardService.getShards(keyspaceName);
  }

  getKeyspaceNames(): Observable<any> {
    return this.http.get(this.keyspacesUrl)
    .map(resp => resp.json());
  }

  getSrvKeyspaces(): Observable<any> {
    return this.http.get(this.srvKeyspaceUrl)
    .map(resp => resp.json());
  }

  /*
    Creates an Observable that fires when both the keyspaceNames and 
    srvkeyspace have been fetched from the server.
  */
  SrvKeyspaceAndNamesObservable(): Observable<any> {
    let keyspaceNamesStream = this.getKeyspaceNames();
    let srvKeyspaceStream = this.getSrvKeyspaces();
    return keyspaceNamesStream.combineLatest(srvKeyspaceStream);
  }

  /*
    Fetches information about a keyspaces shardingColumnName and 
    shardingColumnType.
  */
  getKeyspaceShardingData(keyspaceName: string): Observable<any> {
    return this.http.get(this.keyspacesUrl + keyspaceName)
      .map(resp => resp.json());
  }

  /*
    Creates an Observable that fires when both the shards and sharding
    information for a particular keyspace have been fetched from the server.
  */
  getShardsAndShardingData(keyspaceName: string): Observable<any> {
    let shardsStream = this.getShards(keyspaceName);
    let shardingdataStream = this.getKeyspaceShardingData(keyspaceName);
    return shardsStream.combineLatest(shardingdataStream);
  }

  /*
    Returns an observable that fires a fulled built keyspace from the 
    keyspace's shards, sharding info, and a list of serving shards.
  */
  buildKeyspace(keyspaceName: string, servingShards: string[]): Observable<any> {
    return this.getShardsAndShardingData(keyspaceName)
      .map(streams => {
        let allShards = streams[0];
        let shardingData = streams[1];
        let keyspace = new Keyspace(keyspaceName);
        servingShards.forEach(shard => keyspace.addServingShard(shard));
        allShards.forEach(shard => {
          if (!keyspace.contains(shard)) {
            keyspace.addNonservingShard(shard);
          }
        });
        keyspace.shardingColumnName = shardingData.sharding_column_name || '';
        keyspace.shardingColumnType = shardingData.sharding_column_type || '';
        return keyspace;
      });
  }

  /*
    Returns an array of the names of serving shards for a given keyspace. 
    Bases the list off of the shard references in the master tablet.
  */
  getServingShards(keyspaceName: string, srvKeyspace: any): string[] {
    if (srvKeyspace && srvKeyspace[keyspaceName]) {
      let partitions = srvKeyspace[keyspaceName].partitions;
      if (partitions === undefined) {
        // This happens for redirected keyspaces.
        return [];
      }
      for (let i = 0; i < partitions.length; i++) {
        let partition = partitions[i];
        if (Proto.VT_TABLET_TYPES[partition.served_type] === 'master') {
          let servingShards = partition.shard_references;
          return servingShards ? servingShards.map(shard => { return shard.name; }) : [];
        }
      }
    }
    return [];
  }

  getKeyspaces(): Observable<Observable<any>> {
    return this.SrvKeyspaceAndNamesObservable()
      .map(streams => {
        // CombineLatest creates an Observable that fires an array.
        let keyspaceNames = streams[0];
        // If there are no keyspaces return an Observable to match return type.
        if (keyspaceNames.length < 1) {
          return Observable.from([]);
        }
        let srvKeyspace = streams[1];
        let allKeyspacesStream = undefined;
        keyspaceNames.forEach(keyspaceName => {
          let servingShards = this.getServingShards(keyspaceName, srvKeyspace);
          let keyspaceStream = this.buildKeyspace(keyspaceName, servingShards);
          allKeyspacesStream = allKeyspacesStream ? allKeyspacesStream.merge(keyspaceStream) : keyspaceStream;
        });
        return allKeyspacesStream;
      }
    );
  }

  getKeyspace(keyspaceName: string): Observable<Observable<any>> {
    return this.getSrvKeyspaces()
      .map(srvKeyspace => {
        let servingShards = this.getServingShards(keyspaceName, srvKeyspace);
        return this.buildKeyspace(keyspaceName, servingShards);
      }
    );
  }
}
