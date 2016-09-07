import { Http, URLSearchParams } from '@angular/http';
import { Injectable } from '@angular/core';

@Injectable()
export class TopologyInfoService {
  constructor( private http: Http ) {}

  getCombinedTopologyInfo(keyspace, cell) {
    let params: URLSearchParams = new URLSearchParams();
    params.set('keyspace', keyspace);
    params.set('cell', cell);
    return this.http.get('../api/topology_info/', { search: params })
      .map(resp => resp.json());
  }

  getMetrics() {
    let metrics = ['lag', 'qps', 'health'];
    return metrics;
  }
}
