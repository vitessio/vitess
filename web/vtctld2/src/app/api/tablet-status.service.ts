import { Http, URLSearchParams } from '@angular/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';

import 'rxjs/add/observable/interval';
import 'rxjs/add/operator/switchMap';

@Injectable()
export class TabletStatusService {
  constructor (private http: Http) {}

  getTabletStats(keyspace, cell, tabletType, metric) {
    // params stores the key-value pairs to build the query parameter URL.
    let params: URLSearchParams = new URLSearchParams();
    params.set('keyspace', keyspace);
    params.set('cell', cell);
    params.set('type', tabletType);
    params.set('metric', metric);
    return Observable.interval(1000).startWith(0)
      .switchMap(() => this.http.get('../api/tablet_statuses/', { search: params })
      .map(resp => resp.json()));
  }

  getTabletHealth(cell: string, uid: number) {
    return this.http.get('../api/tablet_health/' + cell + '/' + uid)
      .map(resp => resp.json());
  }
}
