import { Http } from '@angular/http';
import { Injectable } from '@angular/core';

@Injectable()
export class TopologyInfoService {
  constructor( private http: Http ) {}

   getKeyspaces() {
     return this.http.get('/api/keyspaces/')
       .map(resp => resp.json());
   }

   getCells() {
     return this.http.get('/api/cells/')
       .map(resp => resp.json());
   }

   getCombinedTopologyInfo() {
     let keyspaceStream = this.getKeyspaces();
     let cellStream = this.getCells();
     return keyspaceStream.combineLatest(cellStream);
   }

   getTypes() {
     let types = ['MASTER', 'REPLICA', 'RDONLY'];
     return types;
   }

   getMetrics() {
    let metrics = ['lag', 'qps', 'healthy'];
    return metrics;
   }
}
