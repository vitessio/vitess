import { Component, OnInit} from '@angular/core';
import { Workflow, Action, Display, State, ActionState, ActionStyle } from './workflow';
import { WorkflowComponent } from './workflow.component';
import {Accordion, AccordionTab, Header} from 'primeng/primeng';

@Component({
  moduleId: module.id,
  selector: 'vt-tasks',
  templateUrl: './tasks.component.html',
  styleUrls: ['./tasks.component.css', './workflow.component.css'],
  directives: [
    WorkflowComponent,
    Accordion, AccordionTab, Header,
  ],
})

export class TasksComponent implements OnInit {
  title = 'Vitess Control Panel';
  workflows = {
    /*'UU948312': new Workflow('A', 'UU948312', 'UU948312', {
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
    }),*/
    'UU130429': new Workflow('Horizontal Resharding Workflow', 'UU130429', {
      '1': new Workflow('Approval', 'UU130429/1', {}, `Workflow was not started automatically. Click 'Start'.`, State.DONE, 1471234131, Display.NONE, 0, '', false, [new Action('Start', ActionState.ENABLED, ActionStyle.TRIGGERED)], 'Success'),
      '2': new Workflow('Bootstrap', 'UU130429/2', {
        '6': new Workflow('Copy to -80', 'UU130429/2/6', {}, 'Copying data from 0', State.DONE, 1471234150, Display.DETERMINATE, 100, '56372/56372 rows', false, [], 'Success'),
        '7': new Workflow('Copy to 80-', 'UU130429/2/7', {}, 'Copying data from 0', State.DONE, 1471234225, Display.DETERMINATE, 100, '56373/56373 rows', false, [], 'Success')
      }, '', State.DONE, 1471234300, Display.DETERMINATE, 100, '2/2', false, [new Action('Canary 1st Shard', ActionState.ENABLED, ActionStyle.TRIGGERED), new Action('Remaining Shards', ActionState.ENABLED, ActionStyle.TRIGGERED)], 'Success'),
      '3': new Workflow('Diff', 'UU130429/3', {
        '9': new Workflow('Copy to -80', 'UU130429/3/9', {}, 'Comparing -80 and 0', State.DONE, 1471234350, Display.DETERMINATE, 100, '56372/56372 rows', false, [], 'Success'),
        '10': new Workflow('Copy to 80-', 'UU130429/3/10', {}, 'Comparing 80- and 0', State.DONE, 1471234425, Display.DETERMINATE, 100, '56373/56373 rows', false, [], 'Success')
      }, '', State.DONE, 1471234500, Display.DETERMINATE, 100, '2/2', false, [new Action('Canary 1st Shard', ActionState.ENABLED, ActionStyle.TRIGGERED), new Action('Remaining Shards', ActionState.ENABLED, ActionStyle.TRIGGERED)], 'Success'),
      '4': new Workflow('Redirect', 'UU130429/4', {
        '11': new Workflow('Redirect REPLICA', 'UU130429/4/11', {
          '14': new Workflow('Redirect -80', 'UU130429/4/11/14', {}, 'Migrating -80', State.DONE, 1471234650, Display.DETERMINATE, 100, '', false, [], 'Success on tablet 1 \nSuccess on tablet 2 \nSuccess on tablet 3 \nSuccess on tablet 4 \n'),
          '15': new Workflow('Redirect 80-', 'UU130429/4/11/15', {}, 'Migrating -80', State.DONE, 1471234700, Display.DETERMINATE, 100, '', false, [], 'Success on tablet 1 \nSuccess on tablet 2 \nSuccess on tablet 3 \nSuccess on tablet 4 \n'),
        }, 'Migrating Serve Type: REPLICA', State.DONE, 1471234700, Display.DETERMINATE, 100, '2/2', false, [], ''),
        '12': new Workflow('Redirect RDONLY', 'UU130429/4/12', {
          '16': new Workflow('Redirect -80', 'UU130429/4/12/16', {}, 'Migrating -80', State.DONE, 1471234750, Display.DETERMINATE, 100, '', false, [], 'Success on tablet 1 \nSuccess on tablet 2 \nSuccess on tablet 3 \nSuccess on tablet 4 \n'),
          '17': new Workflow('Redirect 80-', 'UU130429/4/12/17', {}, 'Migrating 80-', State.RUNNING, 0, Display.DETERMINATE, 0, '', false, [], ''),
        }, 'Migrating Serve Type: RDONLY', State.RUNNING, 1471234800, Display.DETERMINATE, 50, '1/2', false, [], ''),
        '13': new Workflow('Redirect Master', 'UU130429/4/13', {
          '16': new Workflow('Redirect -80', 'UU130429/4/12/16', {}, 'Migrating -80', State.NOT_STARTED, 0, Display.DETERMINATE, 0, '', false, [], ''),
          '17': new Workflow('Redirect 80-', 'UU130429/4/12/17', {}, 'Migrating 80-', State.NOT_STARTED, 0, Display.DETERMINATE, 0, '', false, [], ''),
        }, 'Migrating Serve Type: MASTER', State.NOT_STARTED, 0, Display.DETERMINATE, 0, '0/2', false, [], ''),
      }, '', State.RUNNING, 1471235000, Display.DETERMINATE, 50, '3/6', false, [new Action('Canary 1st Tablet Type', ActionState.ENABLED, ActionStyle.TRIGGERED), new Action('Remaining Tablet Types', ActionState.ENABLED, ActionStyle.NORMAL)], ''),
      '5': new Workflow('Cleanup', 'UU130429/5', {
        '18': new Workflow('Redirect 80-', 'UU130429/5/18', {}, 'Recursively removing old shards', State.NOT_STARTED, 0, Display.DETERMINATE, 0, '', false, [], ''),
      }, '', State.NOT_STARTED, 0, Display.NONE, 0, '', false, [], ''),
    }, 'PRIYANKA', State.RUNNING, 1471235000, Display.DETERMINATE, 63, '63%', false, [], 'DINOSAURS')
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
    /*this.updateWorkFlow('UU948312', {name: 'Going Back to the Future', state: 1, display: 1, progress: 33, progressMsg: 'Working'});
    this.updateWorkFlow('UU948312/1', {name: 'Acquiring Delorean', display: 1, progress: 100, progressMsg: '1/1', lastChanged: 1471234131, state: 2, message: 'Leased from Doc', disabled: false, actions: [new Action('Return', 0)]});
    this.updateWorkFlow('UU948312/1/4', {state: 2});
    this.updateWorkFlow('UU948312/1/4/7', {state: 2});
    this.updateWorkFlow('UU948312/1/4/8', {state: 2});
    this.updateWorkFlow('UU948312/2/5', {state: 1, display: 0, progressMsg: 'Waiting for a storm'});
    this.updateWorkFlow('UU948312/2/6', {state: 0});
    this.updateWorkFlow('UU948312/2', {name: 'Charge the Flux Capacitor', display: 0, progressMsg: 'Charging', lastChanged: 1471234200, state: 1, message: '', disabled: false, actions: [new Action('Accelerate', 2)]});
    this.updateWorkFlow('UU948312/3', {name: 'Hit 88MPH', message: 'Great Scott!', display: 2, progressMsg: 'Beaming Up'});*/

    //this.updateWorkFlow();
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

