import { Component, OnDestroy, OnInit } from '@angular/core';

import { Observable } from 'rxjs/Observable';

import { Node } from './node';
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
  workflows = [];
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
    // update flags
    for (let key of Object.keys(flags)) {
      newFlags[key].value = flags[key].value;
    }
    // make sure that we only include flags pertinent to current workflow
    let filteredFlags = {};
    let factory_name = newFlags['factory_name'].value;
    // all workflows include factory name and skip start
    filteredFlags['factory_name'] = newFlags['factory_name'];
    filteredFlags['skip_start']= newFlags['skip_start'];
    for (let key of Object.keys(newFlags)) {
        if (key.startsWith(factory_name)) {
            filteredFlags[key]= newFlags[key];
        }
    }

    this.workflowParametersSanitize(filteredFlags);
    return new PrepareResponse(true, filteredFlags);
  }

  workflowParametersSanitize(newFlags) {
    if (newFlags['factory_name'] === 'sleep') {
      newFlags['duration']['value'] = '30';
    }
    if (newFlags['factory_name'] === 'other') {
      newFlags['duration']['value'] = '';
    }
    this.workflowPrepareReshardingFlags(newFlags);
  }

  workflowPrepareReshardingFlags(newFlags) {
    let factoryName = newFlags['factory_name']['value']
      if (factoryName === 'horizontal_resharding' || factoryName == 'hr_workflow_gen') {
        let phaseEnableApprovalCheckBoxNames = [
            'enable_approvals_copy_schema',
            'enable_approvals_clone',
            'enable_approvals_wait_filtered_replication',
            'enable_approvals_diff',
            'enable_approvals_migrate_serving_types',
        ];
        let phaseEnableApprovals = [];

        for (let i=0; i<phaseEnableApprovalCheckBoxNames.length; i++) {
            let phaseName = phaseEnableApprovalCheckBoxNames[i];
            if(newFlags[factoryName + '_' + phaseName]['value']) {
                phaseEnableApprovals.push(newFlags[factoryName + '_' + phaseName]['namedPositional']);
            }
            // We don't want this flag to show up in the getArgs
            delete newFlags[factoryName + '_' + phaseName];
        }
        newFlags[factoryName + '_phase_enable_approvals']['value'] = phaseEnableApprovals.join(',');
    }
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }

  isMaster(): boolean {
    return this.redirect === '';
  }

  sendAction(path: string, name: string) {
    this.workflowService.sendAction(path, name);
  }
}

