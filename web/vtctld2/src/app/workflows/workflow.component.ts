import { Component, Input } from '@angular/core';
import { Node, ActionStyle } from './node';
import { Accordion, AccordionTab, Header } from 'primeng/primeng';

import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { WorkflowFlags } from '../shared/flags/workflow.flags';

import { WorkflowListComponent } from './workflow-list.component';

@Component({
  selector: 'vt-workflow',
  templateUrl: './workflow.component.html',
  styleUrls: ['../styles/vt.style.css', './workflow.component.css'],
  directives: [Accordion, AccordionTab, Header],
})

export class WorkflowComponent {
  @Input() workflow: Node;
  @Input() workflowListComponent: WorkflowListComponent;

  getChildrenIds() {
    return Object.keys(this.workflow.children);
  }

  getTime() {
    if (this.workflow.lastChanged) {
      let d = new Date(this.workflow.lastChanged * 1000);
      return d.toString();
    }
  }

  getState() {
    switch (this.workflow.state) {
      case 0:
        return 'vt-workflow-not-started';
      case 1:
        return 'vt-workflow-running';
      case 2:
        return 'vt-workflow-done';
      default:
        return '';
    }
  }

  getActionClass(state) {
    switch (state) {
      case ActionStyle.NORMAL:
        return 'vt-action-normal';
      case ActionStyle.TRIGGERED:
        return 'vt-action-triggered';
      case ActionStyle.WAITING:
        return 'vt-action-waiting';
      case ActionStyle.WARNING:
        return 'vt-action-warning';
     default:
        return '';
    }
  }

  actionClicked(name) {
    this.workflowListComponent.sendAction(this.workflow.path, name);
  }

  // For the next three methods, we want to do two things with the event:
  // - stop the event from being propagated up the chain. If we let
  //   it go up the chain, it will expand / collapse the accordion,
  //   which is weird.
  //   This is achieved by calling event.stopPropagation() below.
  // - prevent the event default behavior. The default behavior in
  //   this case is to reload the entire page.
  //   This is achieved by returning 'false' in the HTML file click action:
  //   (click)="startClicked($event); false".
  startClicked(event) {
    event.stopPropagation();
    this.workflowListComponent.dialogSettings = new DialogSettings('Start', `Start ${this.workflow.name}`,
                                             `Are you sure you want to start ${this.workflow.name}?`,
                                             `There was a problem starting ${this.workflow.name}:`);
    this.workflowListComponent.dialogSettings.setMessage('Workflow started.');
    let flags = new WorkflowFlags(this.workflow.getId()).flags;
    this.workflowListComponent.dialogContent = new DialogContent('workflow_uuid', flags, {}, undefined, 'WorkflowStart');
    this.workflowListComponent.dialogSettings.toggleModal();
  }

  stopClicked(event) {
    event.stopPropagation();
    this.workflowListComponent.dialogSettings = new DialogSettings('Stop', `Stop ${this.workflow.name}`,
                                             `Are you sure you want to stop ${this.workflow.name}?`,
                                             `There was a problem stopping ${this.workflow.name}:`);
    this.workflowListComponent.dialogSettings.setMessage('Workflow stopped.');
    let flags = new WorkflowFlags(this.workflow.getId()).flags;
    this.workflowListComponent.dialogContent = new DialogContent('workflow_uuid', flags, {}, undefined, 'WorkflowStop');
    this.workflowListComponent.dialogSettings.toggleModal();
  }

  deleteClicked(event) {
    event.stopPropagation();
    this.workflowListComponent.dialogSettings = new DialogSettings('Delete', `Delete ${this.workflow.name}`,
                                             `Are you sure you want to delete ${this.workflow.name}?`,
                                             `There was a problem deleting ${this.workflow.name}:`);
    this.workflowListComponent.dialogSettings.setMessage('Workflow deleted.');
    let flags = new WorkflowFlags(this.workflow.getId()).flags;
    this.workflowListComponent.dialogContent = new DialogContent('workflow_uuid', flags, {}, undefined, 'WorkflowDelete');
    this.workflowListComponent.dialogSettings.toggleModal();
  }
}
