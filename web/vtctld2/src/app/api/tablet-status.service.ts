import { Http, URLSearchParams } from '@angular/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';

import 'rxjs/add/observable/interval';
import 'rxjs/add/operator/switchMap';

const statusUrl = '../api/tablet_statuses/';
const tabletUrl = '../api/tablet_info/';
const metricKey = 'metric';
const keyspaceKey = 'keyspace';
const cellKey = 'cell';
const typeKey = 'type';

@Injectable()
export class TabletStatusService {
  constructor (private http: Http) {}

  getInitialTabletStats(metric, cell, keyspace, tabletType) {
    // params stores the key-value pairs to build the query parameter URL.
    let params: URLSearchParams = new URLSearchParams();
    params.set(metricKey, metric);
    params.set(keyspaceKey, keyspace);
    params.set(cellKey, cell);
    params.set(typeKey, tabletType);
    return this.http.get(statusUrl, { search: params })
      .map( resp => resp.json());
  }

  getTabletStats(metric, cell, keyspace, tabletType) {
    // params stores the key-value pairs to build the query parameter URL.
    let params: URLSearchParams = new URLSearchParams();
    params.set(metricKey, metric);
    params.set(keyspaceKey, keyspace);
    params.set(cellKey, cell);
    params.set(typeKey, tabletType);
    return Observable.interval(10000)
      .switchMap( () => this.http.get(statusUrl, { search: params })
      .map( resp => resp.json()));
  }

  getTabletHealth(cell: string, uid: number) {
    return this.http.get(tabletUrl + cell + '/' + uid)
      .map( resp => resp.json() );
  }
}
