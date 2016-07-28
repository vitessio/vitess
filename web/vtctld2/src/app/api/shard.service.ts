import { Headers, Http, RequestOptions } from '@angular/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';

@Injectable()
export class ShardService {
  private shardsUrl = '../api/shards/';
  constructor(private http: Http) {}

  getShards(keyspaceName: string): Observable<any> {
    return this.http.get(this.shardsUrl + keyspaceName + '/')
      .map(resp => resp.json());
  }

  sendPostRequest(url: string, body: string): Observable<any> {
    let headers = new Headers({ 'Content-Type': 'application/x-www-form-urlencoded' });
    let options = new RequestOptions({ headers: headers });
    return this.http.post(url, body, options)
      .map(resp => resp.json());
  }
  createShard(shard): Observable<any> {
    return this.sendPostRequest(this.shardsUrl + shard.getParam("keyspaceName") + "/" + shard.getParam("shardName")+ "/", shard.getBody("CreateShard"));
  }

  deleteShard(shard): Observable<any> {
    return this.sendPostRequest(this.shardsUrl + shard.getParam("keyspaceName") + "/" + shard.name+ "/", shard.getBody("DeleteShard"));
  }
}