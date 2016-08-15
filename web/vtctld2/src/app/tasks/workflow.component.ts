import { Component, Input } from '@angular/core';
import { Workflow } from './workflow';

import {Accordion, AccordionTab, Header} from 'primeng/primeng';

@Component({
  moduleId: module.id,
  selector: 'vt-workflow',
  templateUrl: './workflow.component.html',
  styleUrls: ['./workflow.component.css'],
  directives: [Accordion, AccordionTab, Header],
})

export class WorkflowComponent {
  @Input() workflow: Workflow;

  console() {
    console.log(Object.keys(this.workflow));
  }

  getChildrenIds() {
    return Object.keys(this.workflow.children);
  }

  getTime() {
    if (this.workflow.lastChanged) {
      let d = new Date(this.workflow.lastChanged * 1000);
      return d.toString();
    }
  }

  getWorkflowClass() {
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

  getActionClass(state) {
    switch (state) {
      case 0:
        return 'vt-action-enabled';
      case 1:
        return 'vt-action-disabled';
      case 2:
        return 'vt-action-warning';
      case 3:
        return 'vt-action-waiting';
     default:
        return '';
    }
  }

  blockClick(e) {
    e.stopPropagation();
  }
}
