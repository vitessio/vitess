import { Component, OnInit} from '@angular/core';
import { Workflow, Action } from './workflow';
import { WorkflowComponent } from './workflow.component';
import {Accordion, AccordionTab, Header} from 'primeng/primeng';

@Component({
  moduleId: module.id,
  selector: 'vt-tasks',
  templateUrl: './tasks.component.html',
  styleUrls: ['./tasks.component.css', './workflow.component.css'],
  directives: [
    WorkflowComponent,
    Accordion, AccordionTab, Header
  ],
})

export class TasksComponent implements OnInit {
  title = 'Vitess Control Panel';
  workflows = {
    'UU948312': new Workflow('A', 'UU948312', 'UU948312', {
      '1': new Workflow('B', '1', 'UU948312/1', {
        '4': new Workflow('Get Doc Plutonium', '3', 'UU948312/1/3', {
          '7': new Workflow('Steal Plutonium from the Libyans', '7', 'UU948312/1/3/7', {}),
          '8': new Workflow('Escape from Libyans', '8', 'UU948312/1/3/8', {}),
        })
      }),
      '2': new Workflow('C', '2', 'UU948312/2', {
        '5': new Workflow('Waiting on Lightning', '5', 'UU948312/2/5', {}),
        '6': new Workflow('Transfer Power', '6', 'UU948312/2/6', {}),
      }),
      '3': new Workflow('C', '2', 'UU948312/2', {}),
    }),
    'UU130429': new Workflow('Horizontal Resharding Workflow', 'UU948312', 'UU948312', {
      '1': new Workflow('Approval', '1', 'UU130429/1', {}, `Workflow was not started automatically. Click 'Start'.`, 2),
      '2': new Workflow('Bootstrap', '2', 'UU130429/2', {}, '', 2),
      '3': new Workflow('Cloning', '3', 'UU130429/3', {}, '', 2),
      '4': new Workflow('Diff', '4', 'UU130429/4', {}, '', 2),
      '5': new Workflow('Redirect', '5', 'UU130429/5', {}, '', 2),
      '6': new Workflow('Cleanup', '6', 'UU130429/6', {}, '', 2),
    }, '', 1)
  };

  /*
  public lastChanged: number; // Time last changed in seconds.
  public progress: number; // Should be an int from 0-100 for percentage
  public progressMsg: string; // Ex. “34/256” “25%” “calculating”  
  public state: State;
  public display: Display;
  public message: String; // Instructions for user
  public disabled: boolean; // Use for blocking further actions
  public actions: Action[];

  */

  ngOnInit() {
    this.updateWorkFlow('UU948312', {name: 'Going Back to the Future', state: 1, display: 1, progress: 33, progressMsg: 'Working'});
    this.updateWorkFlow('UU948312/1', {name: 'Acquiring Delorean', display: 1, progress: 100, progressMsg: '1/1', lastChanged: 1471234131, state: 2, message: 'Leased from Doc', disabled: false, actions: [new Action('Return', 0)]});
    this.updateWorkFlow('UU948312/1/4', {state: 2});
    this.updateWorkFlow('UU948312/1/4/7', {state: 2});
    this.updateWorkFlow('UU948312/1/4/8', {state: 2});
    this.updateWorkFlow('UU948312/2/5', {state: 1, display: 0, progressMsg: 'Waiting for a storm'});
    this.updateWorkFlow('UU948312/2/6', {state: 0});
    this.updateWorkFlow('UU948312/2', {name: 'Charge the Flux Capacitor', display: 0, progressMsg: 'Charging', lastChanged: 1471234200, state: 1, message: '', disabled: false, actions: [new Action('Accelerate', 2)]});
    this.updateWorkFlow('UU948312/3', {name: 'Hit 88MPH', message: 'Great Scott!', display: 2, progressMsg: 'Beaming Up'});
  }

  getWorkflowIds() {
    return Object.keys(this.workflows);
  }

  getWorkflow(path) {
    let tokens = path.split('/');
    let target = this.workflows;
    let numToks = tokens.length;
    for (let i = 0; i < (numToks - 1); i ++) {
      if (target) {
        target = target[tokens[i]].children;
      } else {
        return target;
      }
    }
    target = target[tokens[numToks - 1]];
    return target;
  }

  buildWorkflow(path) {
    let tokens = path.split('/');
    let target = this.workflows;
    let numToks = tokens.length;
    for (let i = 0; i < (numToks - 1); i ++) {
      let newTarget = target[tokens[i]];
      if (!newTarget) {
        target[tokens[i]] = {children: {}};
        newTarget = target[tokens[i]];
      }
      target = newTarget.children;
    }
    target[tokens[numToks - 1]] = {};
    return target[tokens[numToks - 1]];
  }

  updateWorkFlow(path: string, changes) {
    let target = this.getWorkflow(path);
    if (target) {
      let keys = Object.keys(changes);
      for (let key of keys) {
        target[key] = changes[key];
      }
    }
  }

  getHeaderClass(state) {
    switch (state) {
      case 0:
        return 'vt-workflow-not-started-dark';
      case 1:
        return 'vt-workflow-running-dark';
      case 2:
        return 'vt-workflow-done-dark';
      default:
        return '';
    }
  }
}

