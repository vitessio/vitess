import { Http } from '@angular/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Rx';
@Injectable()
export class WorkflowService {
  private workflows = {
    'UU948312': {name: 'Going Back to the Future', path: '/UU948312', children: {
      '1': {name: 'Acquiring Delorean', path: '/UU948312/1', children: {
        '4': {name: 'Get Doc Plutonium', path: '/UU948312/1/3', children: {
          '7': {name: 'Steal Plutonium from the Libyans', path: '/UU948312/1/3/7', children: {}, state: 2},
          '8': {name: 'Escape from Libyans', path: '/UU948312/1/3/8', children: {}, state: 2},
        }, state: 2}
      }, display: 1, progress: 100, progressMsg: '1/1', lastChanged: 1471234131000, state: 2, message: 'Leased from Doc', disabled: false, actions: [{name: 'Return', state: 0, style: 0}]},
      '2': {name: 'Charge the Flux Capacitor', path: '/UU948312/2', children: {
        '5': {name: 'Waiting on Lightning', path: '/UU948312/2/5', children: {}, state: 1, display: 0, progressMsg: 'Waiting for a storm'},
        '6': {name: 'Transfer Power', path: '/UU948312/2/6', children: {}, state: 0},
      }, display: 0, progressMsg: 'Charging', lastChanged: 147123420000, state: 1, message: '', disabled: false, actions: [{name: 'Accelerate', state: 0, style: 1}]},
      '3': {name: 'Hit 88MPH', path: '/UU948312/3', children: {}, message: 'Great Scott!', display: 2, progressMsg: 'Beaming Up'},
    }, state: 1, display: 1, progress: 33, progressMsg: 'Working'},
  };


  constructor(private http: Http) {}

  getWorkflows() {
    return Observable.create(observer => {
      observer.next(this.workflows);
      observer.complete();
    });
  }
}

