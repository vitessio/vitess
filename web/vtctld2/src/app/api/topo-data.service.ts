import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';

import { Observable }     from 'rxjs/Observable';

@Injectable()
export class TopoDataService {
  constructor(private http: Http) { }

  private apiUrl = '../api/topodata/';

  getNode(path: string): Observable<any> {
    return this.http.get(this.apiUrl + path)
      .map(response => response.json())
      .catch(this.handleError);
  }

  private handleError(error: any) {
    let errMsg = (error.message) ? error.message :
      error.status ? `${error.status} - ${error.statusText}` : 'Server error';
    console.error(errMsg);
    return Observable.throw(errMsg);
  }
}
