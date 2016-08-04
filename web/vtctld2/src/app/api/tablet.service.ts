import { Http } from '@angular/http';
import { Injectable } from '@angular/core';

const statusUrl = './tablet_statuses/';

@Injectable()
export class TabletService {

  constructor (private http: Http) {}

  getTablets(cell, keyspace, shard, tabletType) {
    return this.http.get(statusUrl + cell + '/' + keyspace + '/' + shard + '/' + tabletType)
      .map( resp => resp.json());
  }
}
