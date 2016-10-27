import { Injectable } from '@angular/core';
import { Headers, Http, RequestOptions, Response } from '@angular/http';

import { ReplaySubject } from 'rxjs/Rx';

@Injectable()
export class WorkflowService {
  private useWebSocket = false;
  private subject: ReplaySubject<any> = new ReplaySubject<any>(100);
  private stopped = false;

  // If using web sockets
  private ws: WebSocket = null;

  // If using HTTP
  private id = 0;

  /*
  private workflows = [
    {name: 'Going Back to the Future', path: '/UU948312', children: [
      {name: 'Acquiring Delorean', path: '/UU948312/1', children: [
        {name: 'Get Doc Plutonium', path: '/UU948312/1/3', children: [
          {name: 'Steal Plutonium from the Libyans', path: '/UU948312/1/3/7', children: [], state: 2},
          {name: 'Escape from Libyans', path: '/UU948312/1/3/8', children: [], state: 2},
        ], state: 2}
      ], display: 1, progress: 100, progressMsg: '1/1', lastChanged: 1471234131000, state: 2, message: 'Leased from Doc', disabled: false,
         actions: [{name: 'Return', state: 0, style: 0}]},
      {name: 'Charge the Flux Capacitor', path: '/UU948312/2', children: [
        {name: 'Waiting on Lightning', path: '/UU948312/2/5', children: [], state: 1, display: 0, progressMsg: 'Waiting for a storm'},
        {name: 'Transfer Power', path: '/UU948312/2/6', children: [], state: 0},
      ], display: 0, progressMsg: 'Charging', lastChanged: 147123420000, state: 1, message: '', disabled: false,
         actions: [{name: 'Accelerate', state: 0, style: 1}]},
      {name: 'Hit 88MPH', path: '/UU948312/3', children: [], message: 'Great Scott!', display: 2, progressMsg: 'Beaming Up'},
    ], state: 1, display: 1, progress: 33, progressMsg: 'Working'},
  ];

  private overrideWorkflow = [{name: 'Get Doc Thorium', path: '/UU948312/1/3', children: [
          {name: 'Steal Thorium from the Australians', path: '/UU948312/1/3/7', children: [], state: 2},
          {name: 'Escape from Australians', path: '/UU948312/1/3/8', children: [], state: 2},
        ], state: 2}];

  */
  constructor(private http: Http) {
    if (this.useWebSocket) {
      this.connectWebSocket();
    } else {
      this.connectHttp();
    }
  }

  connectWebSocket() {
    if (this.stopped) {
      return;
    }

    // Save a copy of the pointer to our object. 'this' will be out of
    // scope in the websocket callbacks below.
    let t = this;

    let ws = new WebSocket('ws://' + window.location.hostname +
                           ':' + window.location.port + '/api/workflow');
    ws.onopen = function(e: MessageEvent) {
      t.ws = ws;
    };
    ws.onmessage = function(e: MessageEvent) {
      return t.subject.next(JSON.parse(e.data));
    };
    ws.onclose = function(e: CloseEvent) {
      console.log('Disconnected from server, trying again in 3s.');
      t.ws = null;
      setTimeout(() => {
        t.connectWebSocket();
      }, 3000);
    };
  }

  connectHttp() {
    if (this.stopped) {
      return;
    }

    // Save a copy of the pointer to our object. 'this' will be out of
    // scope in the HTTP callbacks below.
    let t = this;

    this.http.get('../api/workflow/create').subscribe(
      function (res: Response) {
        let createResult = res.json();
        t.subject.next(createResult);
        if ('index' in createResult) {
          t.id = createResult.index;
          t.pollHttp();
        } else {
          setTimeout(() => {
            t.connectHttp();
          }, 3000);
        }
      },
      function (err) {
        console.log('Workflow service create error, will try again in 3s: %s', err);
        setTimeout(() => {
          t.connectHttp();
        }, 3000);
      });
  }

  pollHttp() {
    if (this.stopped) {
      return;
    }

    // Save a copy of the pointer to our object. 'this' will be out of
    // scope in the HTTP callbacks below.
    let t = this;

    this.http.get('../api/workflow/poll/' + this.id).subscribe(
      function (res: Response) {
        t.subject.next(res.json());
        t.pollHttp();
      },
      function (err) {
        console.log('Workflow service poll error, will try again in 3s: %s', err);
        t.id = 0;
        setTimeout(() => {
          t.connectHttp();
        }, 3000);
      });
  }

  updates(): ReplaySubject<any> {
    return this.subject;
  }

  sendAction(path: string, name: string) {
    let params = {
      path: path,
      name: name,
    };
    let message = JSON.stringify(params);
    if (this.useWebSocket) {
      this.sendActionWebSocket(message);
    } else {
      this.sendActionHttp(message);
    }
  }

  sendActionWebSocket(message: string) {
    if (this.ws === null) {
      alert('Not connected to vtctld via WebSocket, cannot send command.');
      return;
    }
    this.ws.send(message);
  }

  sendActionHttp(message: string) {
    if (this.id === 0) {
      alert('Not connected to vtctld via HTTP, cannot send command.');
      return;
    }

    let headers = new Headers({
      'Content-Type': 'application/json; charset=utf-8' });
    let options = new RequestOptions({ headers: headers });
    this.http.post('../api/workflow/action/' + this.id, message, options).subscribe(
      function (res: Response) {},
      function (err) {
        alert('Error posting action to server: ' + err);
      });
  }

  stop() {
    this.stopped = true;
    if (this.ws !== null) {
      this.ws.close();
    }
  }
}

