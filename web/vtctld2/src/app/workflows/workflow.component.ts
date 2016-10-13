import { Component, Input } from '@angular/core';
import { Node, ActionStyle } from './node';
import { Accordion, AccordionTab, Header } from 'primeng/primeng';

import { WorkflowService } from '../api/workflow.service';

@Component({
  selector: 'vt-workflow',
  templateUrl: './workflow.component.html',
  styleUrls: ['./workflow.component.css'],
  directives: [Accordion, AccordionTab, Header],
})

export class WorkflowComponent {
  @Input() workflow: Node;
  @Input() workflowService: WorkflowService;

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

  blockClick(e) {
    e.stopPropagation();
    e.preventDefault();
  }

  actionClicked(name) {
    console.log('ActionClicked(%s,%s)', this.workflow.path, name);
    this.workflowService.sendAction(this.workflow.path, name);
  }
}
