import { Http } from '@angular/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Rx';
@Injectable()
export class WorkflowService {
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


  constructor(private http: Http) {}

  getWorkflows() {
    return Observable.create(observer => {
      observer.next(this.workflows);
      observer.next(this.overrideWorkflow);
      observer.complete();
    });
  }
}

