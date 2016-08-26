import { Headers, Http, RequestOptions } from '@angular/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';

@Injectable()
export class VtctlService {
  private vtctlUrl = '../api/vtctl/';
  constructor(private http: Http) {}

  private sendPostRequest(url: string, body: string[]): Observable<any> {
    let headers = new Headers({ 'Content-Type': 'application/json' });
    let options = new RequestOptions({ headers: headers });
    return this.http.post(url, JSON.stringify(body), options)
      .map(resp => resp.json());
  }

  public runCommand(body: string[]): Observable<any> {
    return this.sendPostRequest(this.vtctlUrl, body);
  }
}
