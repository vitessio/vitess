import { Component, OnDestroy, OnInit } from '@angular/core';

import { Observable } from 'rxjs/Observable';

import { Node, Action, Display, State, ActionState, ActionStyle } from './node';
import { WorkflowComponent } from './workflow.component';
import { Accordion, AccordionTab, Header } from 'primeng/primeng';
import { FeaturesService } from '../api/features.service';
import { WorkflowService } from '../api/workflow.service';
import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { PrepareResponse } from '../shared/prepare-response';
import { NewWorkflowFlags } from '../shared/flags/workflow.flags';

@Component({
  selector: 'vt-tasks',
  templateUrl: './workflow-list.component.html',
  styleUrls: ['../styles/vt.style.css', './workflow-list.component.css', './workflow.component.css'],
  directives: [
    WorkflowComponent,
    Accordion, AccordionTab, Header,
  ],
  providers: [WorkflowService],
})

export class WorkflowListComponent implements OnDestroy, OnInit {
  title = 'Workflows';
  redirect = '';
  workflows = [
    new Node('Horizontal Resharding Workflow', '/UU130429', [
      new Node('Approval', '/UU130429/1', []),
      new Node('Bootstrap', '/UU130429/2', [
        new Node('Copy to -80', '/UU130429/2/6', []),
        new Node('Copy to 80-', '/UU130429/2/7', [])
      ]),
      new Node('Diff', '/UU130429/3', [
        new Node('Copy to -80', '/UU130429/3/9', []),
        new Node('Copy to 80-', '/UU130429/3/10', [])
      ]),
      new Node('Redirect', '/UU130429/4', [
        new Node('Redirect REPLICA', '/UU130429/4/11', [
          new Node('Redirect -80', '/UU130429/4/11/14', []),
          new Node('Redirect 80-', '/UU130429/4/11/15', []),
        ]),
        new Node('Redirect RDONLY', '/UU130429/4/12', [
          new Node('Redirect -80', '/UU130429/4/12/16', []),
          new Node('Redirect 80-', '/UU130429/4/12/17', []),
        ]),
        new Node('Redirect Master', '/UU130429/4/13', [
          new Node('Redirect -80', '/UU130429/4/13/16', []),
          new Node('Redirect 80-', '/UU130429/4/13/17', []),
        ]),
      ]),
      new Node('Cleanup', '/UU130429/5', [
        new Node('Redirect 80-', '/UU130429/5/18', []),
      ]),
    ]),
    new Node('TEST DUMMY', '/UU948312', [])
  ];
  dialogSettings: DialogSettings;
  dialogContent: DialogContent;

  constructor(
    private featuresService: FeaturesService,
    private workflowService: WorkflowService) {}

  ngOnInit() {
    this.workflowService.updates().subscribe(update => {
      this.processUpdateJson(update);
    });
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();

    // Resharding Workflow Example
    this.updateWorkFlow('/UU130429', {
      message: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt\
ut labore et dolore magna aliqua.',
      state: State.RUNNING,
      lastChanged: 1471235000000,
      display: Display.DETERMINATE,
      progress: 63,
      progressMsg: '63%',
      disabled: false,
      log: 'Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque \
laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi \
architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas \
sit aspernatur aut odit aut fugit, sed quia conseq/UUntur magni dolores eos qui ratione \
voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, \
consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et \
dolore magnam aliquam quaerat voluptatem.'});
    this.updateWorkFlow('/UU130429/1', {
      message: `Workflow was not started automatically. Click 'Start'.`,
      state: State.DONE,
      lastChanged: 1471234131000,
      actions: [new Action('Start', ActionState.ENABLED, ActionStyle.TRIGGERED)],
      log: 'Started'});
    this.updateWorkFlow('/UU130429/2/6', {
      message: 'Copying data from 0',
      state: State.DONE,
      lastChanged: 1471234150000,
      display: Display.DETERMINATE,
      progress: 100,
      progressMsg: '56372/56372 rows',
      log: 'Success'});
    this.updateWorkFlow('/UU130429/2/7', {
      message: 'Copying data from 0',
      state: State.DONE,
      lastChanged: 1471234225000,
      display: Display.DETERMINATE,
      progress: 100,
      progressMsg: '56373/56373 rows',
      log: 'Success'});
    this.updateWorkFlow('/UU130429/2', {
      state: State.DONE,
      lastChanged: 1471234300000,
      display: Display.DETERMINATE,
      progress: 100,
      progressMsg: '2/2',
      actions: [
        new Action('Canary 1st Shard', ActionState.ENABLED, ActionStyle.TRIGGERED),
        new Action('Remaining Shards', ActionState.ENABLED, ActionStyle.TRIGGERED)],
      log: 'Success'});
    this.updateWorkFlow('/UU130429/3/9', {
      message: 'Comparing -80 and 0',
      state: State.DONE,
      lastChanged: 1471234350000,
      display: Display.DETERMINATE,
      progress: 100,
      progressMsg: '56372/56372 rows',
      log: 'Success'});
    this.updateWorkFlow('/UU130429/3/10', {
      message: 'Comparing 80- and 0',
      state: State.DONE,
      lastChanged: 1471234425000,
      display: Display.DETERMINATE,
      progress: 100,
      progressMsg: '56373/56373 rows',
      log: 'Success'});
    this.updateWorkFlow('/UU130429/3', {
      state: State.DONE,
      lastChanged: 1471234500000,
      display: Display.DETERMINATE,
      progress: 100,
      progressMsg: '2/2',
      actions: [
        new Action('Canary 1st Shard', ActionState.ENABLED, ActionStyle.TRIGGERED),
        new Action('Remaining Shards', ActionState.ENABLED, ActionStyle.TRIGGERED)],
      log: 'Success'});
    this.updateWorkFlow('/UU130429/4/11', {
      message: 'Migrating Serve Type: REPLICA',
      state: State.DONE,
      lastChanged: 1471234700000,
      display: Display.DETERMINATE,
      progress: 100,
      progressMsg: '2/2'});
    this.updateWorkFlow('/UU130429/4/11/14', {
      message: 'Migrating -80',
      state: State.DONE,
      lastChanged: 1471234650000,
      display: Display.DETERMINATE,
      progress: 100,
      log: 'Success on tablet 1 \nSuccess on tablet 2 \nSuccess on tablet 3 \nSuccess on tablet 4 \n'});
    this.updateWorkFlow('/UU130429/4/11/15', {
      message: 'Migrating 80-',
      state: State.DONE,
      lastChanged: 1471234700000,
      display: Display.DETERMINATE,
      progress: 100,
      log: 'Success on tablet 1 \nSuccess on tablet 2 \nSuccess on tablet 3 \nSuccess on tablet 4 \n'});
    this.updateWorkFlow('/UU130429/4/12', {
      message: 'Migrating Serve Type: RDONLY',
      state: State.RUNNING,
      lastChanged: 1471234800000,
      display: Display.DETERMINATE,
      progress: 50,
      progressMsg: '1/2'});
    this.updateWorkFlow('/UU130429/4/12/16', {
      message: 'Migrating -80',
      state: State.DONE,
      lastChanged: 1471234750000,
      display: Display.DETERMINATE,
      progress: 100,
      log: 'Success on tablet 1 \nSuccess on tablet 2 \nSuccess on tablet 3 \nSuccess on tablet 4 \n'});
    this.updateWorkFlow('/UU130429/4/12/17', {
      message: 'Migrating 80-',
      state: State.RUNNING,
      display: Display.DETERMINATE});
    this.updateWorkFlow('/UU130429/4/13', {
      message: 'Migrating Serve Type: MASTER',
      display: Display.DETERMINATE,
      progressMsg: '0/2'});
    this.updateWorkFlow('/UU130429/4/13/16', {
      message: 'Migrating -80',
      display: Display.DETERMINATE});
    this.updateWorkFlow('/UU130429/4/13/17', {
      message: 'Migrating 80-',
      display: Display.DETERMINATE});
    this.updateWorkFlow('/UU130429/4', {
      message: '',
      state: State.RUNNING,
      lastChanged: 1471235000000,
      display: Display.DETERMINATE,
      progress: 50,
      progressMsg: '3/6',
      actions: [
        new Action('Canary 1st Tablet Type', ActionState.ENABLED, ActionStyle.TRIGGERED),
        new Action('Remaining Tablet Types', ActionState.ENABLED, ActionStyle.NORMAL)]});
    this.updateWorkFlow('/UU130429/5/18', {
      message: 'Recursively removing old shards',
      display: Display.DETERMINATE});
  }

  ngOnDestroy() {
    this.workflowService.stop();
  }

  processUpdateJson(update: any) {
    if ('fullUpdate' in update && update.fullUpdate) {
      this.redirect = '';
      this.workflows = [];
    }
    if ('nodes' in update && update.nodes !== null) {
      this.processWorkflowJson(update.nodes);
    }
    if ('deletes' in update && update.deletes !== null) {
      for (let path of update.deletes) {
        this.deleteRootWorkflow(path);
      }
    }
    if ('redirect' in update && update.redirect !== '') {
      this.redirect = update.redirect;
      this.workflows = [];
    }
  }

  processWorkflowJson(workflows: any) {
    // Iterate over all workflows
    for (let workflowData of workflows) {
      if (workflowData.path.charAt(0) !== '/') {
        console.error('The path provided was not absolute.');
        continue;
      }

      let target = this.getWorkflow(workflowData.path);
      if (!target) {
        // The target doesn't exist. It can only happen for new root nodes.
        if (workflowData.path.split('/').length === 2) {
          this.workflows.push(this.recursiveWorkflowBuilder(workflowData));
        } else {
          console.error('Could not find node to update');
        }
        continue;
      }

      // Need to update the target now.
      target.update(workflowData);
      if ('children' in workflowData && workflowData.children !== null) {
        target.children = [];
        for (let childData of workflowData.children) {
          let child = this.recursiveWorkflowBuilder(childData);
          if (child) {
            target.children.push(child);
          }
        }
      }
    }
  }

  setRootWorkflow(path, workflow) {
    for (let i = 0; i < this.workflows.length; i++) {
      if (path === this.workflows[i].path) {
        this.workflows.splice(i, 1);
        break;
      }
    }
    this.workflows.push(workflow);
  }

  deleteRootWorkflow(path) {
    for (let i = 0; i < this.workflows.length; i++) {
      if (path === this.workflows[i].path) {
        this.workflows.splice(i, 1);
        break;
      }
    }
  }

  recursiveWorkflowBuilder(workflowData): Node {
    if (workflowData.name && workflowData.path) {
      let workflow = new Node(workflowData.name, workflowData.path, []);
      // Most data can be directly set using update.
      workflow.update(workflowData);
      // Children must be set recursively
      workflow.children = [];
      if (workflowData.children) {
        for (let childData of workflowData.children) {
          let child = this.recursiveWorkflowBuilder(childData);
          if (child) {
            workflow.children.push(child);
          }
        }
      }
      return workflow;
    }
    return undefined;
  }

  getWorkflow(path, relativeWorkflow= undefined) {
    let target = relativeWorkflow;
    // Check is path is relative or absolute.
    if (this.pathIsAbsolute(path)) {
      target = {children: this.workflows};
      path = path.substring(1);
    }

    // Clean any trailing slashes.
    path = this.cleanPath(path);

    let tokens = path.split('/');
    for (let i = 0; i < tokens.length; i++) {
      if (target) {
        target = this.getChild(tokens[i], target.children);
      } else {
        return target;
      }
    }
    return target;
  }

  pathIsAbsolute(path) {
    return path.length > 0 && path.charAt(0) === '/';
  }

  cleanPath(path) {
    if (path.length > 0 && path.charAt(path.length - 1) === '/') {
      return path.substring(0, path.length - 1);
    }
    return path;
  }

  getChild(id, children) {
    for (let child of children) {
      if (child.getId() === id) {
        return child;
      }
    }
    return undefined;
  }

  updateWorkFlow(path: string, changes) {
    let target = this.getWorkflow(path);
    if (target) {
      target.update(changes);
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

  openNewDialog() {
    this.dialogSettings = new DialogSettings('Create', 'Create a new Workflow', '', 'There was a problem creating {{factory_name}}:');
    this.dialogSettings.setMessage('Created {{factory_name}}');
    let flags = new NewWorkflowFlags(this.featuresService.workflows).flags;
    this.dialogContent = new DialogContent('factory_name', flags, {'factory_name': true}, this.prepareNew.bind(this), 'WorkflowCreate');
    this.dialogSettings.toggleModal();
  }

  prepareNew(flags) {
    let newFlags = new NewWorkflowFlags(this.featuresService.workflows).flags;
    for (let key of Object.keys(flags)) {
      newFlags[key].value = flags[key].value;
    }
    this.workflowParametersSanitize(newFlags);
    return new PrepareResponse(true, newFlags);
  }

  workflowParametersSanitize(newFlags) {
    if (newFlags['factory_name'] === 'sleep') {
      newFlags['duration']['value'] = '30';
    }
    if (newFlags['factory_name'] === 'other') {
      newFlags['duration']['value'] = '';
    }
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}

