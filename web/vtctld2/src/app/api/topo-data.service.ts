import { Http } from '@angular/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';

@Injectable()
export class TopoDataService {
  private apiUrl = '../api/topodata';

  constructor(private http: Http) { }

  getNode(path: string): Observable<any> {
    if (!path.startsWith('/')) {
      path = '/' + path;
    }
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
