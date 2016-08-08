import { Http, URLSearchParams } from '@angular/http';
import { Injectable } from '@angular/core';

const statusUrl = '../api/tablet_statuses/';
const metricKey = 'metric';
const keyspaceKey = 'keyspace';
const cellKey = 'cell';
const typeKey = 'type';

@Injectable()
export class TabletService {
  constructor (private http: Http) {}

  getTabletStats(metric, cell, keyspace, tabletType) {
    // params stores the key-value pairs to build the query parameter URL.
    let params: URLSearchParams = new URLSearchParams();
    params.set(metricKey, metric);
    params.set(keyspaceKey, keyspace);
    params.set(cellKey, cell);
    params.set(typeKey, tabletType);

    return this.http.get(statusUrl, { search: params })
      .map( resp => resp.json());
  }
}
