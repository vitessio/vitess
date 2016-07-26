import { Injectable, Inject } from '@angular/core';
import { Http, Headers, RequestOptions } from '@angular/http';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class ShardService {
  private shardsUrl = '../api/shards/';
  constructor(private http: Http) {}



  getShards(keyspaceName) {
    return this.http.get(this.shardsUrl + keyspaceName + "/")
    .map( (resp) => {
      return resp.json(); 
    })
  }

  sendPostRequest(url, body) {
    let headers = new Headers({ 'Content-Type': 'application/x-www-form-urlencoded' });
    let options = new RequestOptions({ headers: headers });
    return this.http.post(url, body, options)
    .map( (resp) => {
      return resp.json(); 
    });
  }
  createShard(shard) {
    return this.sendPostRequest(this.shardsUrl + shard.getParam("ShardName"), shard.getBody("CreateKeyspace"));
  }
  deleteShard(shard) {
    return this.sendPostRequest(this.shardsUrl + shard.name, shard.getBody("DeleteKeyspace"));
  }
}