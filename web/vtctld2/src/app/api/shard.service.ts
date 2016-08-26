import { Http } from '@angular/http';
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
}
