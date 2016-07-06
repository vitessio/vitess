import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Keyspace } from '../shared/keyspaceObject/keyspace'

@Injectable()
export class TabletService {
  private tabletsUrl = '/app/tablets';

  constructor(private http: Http) {}


  getTablets(keyspaceName, shardName) {
    return this.http.get(this.tabletsUrl + '/?KSName=' + keyspaceName)
    .map( (resp) => {
      return resp.json().data;
    })
    .map ( (keyspace: any) => {
      if (keyspace.length < 1) return [];
      var shards = keyspace[0].shards;
      for (var i = 0; i < shards.length; i++) {
        if (shards[i].name == shardName){
          return shards[i].tablets;
        }
      }
      return [];
    })
  }
}