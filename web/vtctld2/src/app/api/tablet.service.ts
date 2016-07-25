import { Injectable } from '@angular/core';
import { Http, Headers, RequestOptions } from '@angular/http';
import { Tablet } from './tablet';
import {Observable} from 'rxjs/Observable';

@Injectable()
export class TabletService {

 private statusUrl = './tablet_statuses/';

  constructor (private http: Http) {}

  getTablets(cell, ks, shard, type) {
    return this.http.get(this.statusUrl + cell + "/" + ks + "/" + shard + "/" + type)
    .map( (resp) => {
      console.log("RETURNED: " + resp.json());
      return resp.json();
    })
  }

}


