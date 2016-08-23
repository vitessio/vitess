import { Headers, Http, RequestOptions } from '@angular/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';

@Injectable()
export class VtctlService {
  public vtctlUrl = '../api/vtctl/';
  constructor(private http: Http) {}

  sendPostRequest(url: string, body: string[]): Observable<any> {
    let headers = new Headers({ 'Content-Type': 'application/json' });
    let options = new RequestOptions({ headers: headers });
    return this.http.post(url, JSON.stringify(body), options)
      .map(resp => resp.json());
  }

  vtctlRunCommand(dialogContent: any, action: string): Observable<any> {
    return this.sendPostRequest(this.vtctlUrl, dialogContent.getPostBody(action));
  }

  serverCall(action: string, dialogContent: any, dialogSettings: any, errorMessage: string) {
    dialogSettings.startPending();
    this.vtctlRunCommand(dialogContent, action).subscribe(resp => {
      if (resp.Error) {
        dialogSettings.setMessage(`${errorMessage} ${resp.Error}`);
      }
      dialogSettings.setLog(resp.Output);
      dialogSettings.endPending();
    });
  }
}
