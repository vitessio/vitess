import { Injectable } from '@angular/core';
import { Headers, Http, RequestOptions } from '@angular/http';

import { Observable } from 'rxjs/Observable';

import { Proto } from '../shared/proto';

@Injectable()
export class TabletService {
  private tabletsUrl = '../api/tablets/';

  constructor(private http: Http) {}
  getTabletList(shardRef: string): Observable<any> {
    return this.sendUrlPostRequest(this.tabletsUrl, `shard=${shardRef}`);
  }

  getTablet(tabletRef: string): Observable<any> {
    return this.http.get(this.tabletsUrl + tabletRef)
      .map(tabletJson => {
        let tablet = tabletJson.json();
        tablet['type'] = Proto.VT_TABLET_TYPES[tablet['type']];
        tablet['label'] = `${tablet['type']}(${tablet['alias']['uid']})`;
        tablet['cell'] = tablet['alias']['cell'];
        tablet['uid'] = tablet['alias']['uid'];
        tablet['alias'] = `${tablet['cell']}-${tablet['uid']}`;
        return tablet;
      });
  }

  // Send a post request using url encoding.
  sendUrlPostRequest(url: string, body: string): Observable<any> {
    let headers = new Headers({ 'Content-Type': 'application/x-www-form-urlencoded' });
    let options = new RequestOptions({ headers: headers });
    return this.http.post(url, body, options)
      .map(resp => resp.json());
  }
}
