import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Tablet } from '../tabletObject/tablet';

@Injectable()
export class TabletService {
  private targetUrl = '/app/tablet_statuses';

  constructor(private http: Http) {}

  getTablets() {
    return this.http.get(this.targetUrl)
    .map( (resp) => {
      return resp.json();
    })
    .map( (target:any) => {
       return target.data;
    })
  }
}
