import { Injectable } from '@angular/core';
import { Http } from '@angular/http';

import { Observable } from 'rxjs/Observable';

@Injectable()
export class TabletService {
  private tabletsUrl = '/app/tablets/';

  constructor(private http: Http) {}

  getTablets(keyspaceName, shardName): Observable<any> {
    return this.http.get(this.tabletsUrl + '?KSName=' + keyspaceName)
      .map(resp => resp.json())
      .map(keyspace => {
        if (keyspace.length < 1) {
          return [];
        }
        let shards = keyspace[0].shards;
        for (let i = 0; i < shards.length; i++) {
          if (shards[i].name === shardName) {
            return shards[i].tablets;
          }
        }
        return [];
      });
  }
}
