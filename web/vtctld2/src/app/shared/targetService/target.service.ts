import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Tablet } from '../tabletObject/tablet';

@Injectable()
export class TargetService { 
  private keyspacesUrl = '/app/keyspaces';

  constructor(private http: Http) {}

  getTablets() {
    /*return this.http.get(this.targetUrl)
    .map( (resp) => {
      return resp.json();
    })*/

    return this.http.get(this.keyspacesUrl)
    .map( (resp) => {
      return resp.json();
    })
    .map ( (keyspaces: any) => {
      return keyspaces.data;
    })
  }
}
