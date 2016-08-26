import { Component, ComponentResolver, EventEmitter, Input, Output } from '@angular/core';

import { DialogContent } from './dialog-content';
import { DialogSettings } from './dialog-settings';

@Component({
  selector: 'vt-dialog',
  templateUrl: './dialog.component.html',
  styleUrls: ['./dialog.component.css', '../../styles/vt.style.css'],
})
export class DialogComponent {
  keyspaces = [];
  extraContentReference: any;
  @Input() dialogContent: DialogContent;
  @Input() dialogSettings: DialogSettings;
  @Output() close = new EventEmitter();

  constructor(private componentResolver: ComponentResolver) {}

  typeSelected(paramName, e) {
    // Polymer event syntax, waiting on Material2 implementation of dropdown.
    this.dialogContent.setParam(paramName, e.detail.item.__dom.firstChild.data);
  }

  cancelDialog() {
    this.dialogSettings.toggleModal();
    this.close.emit({});
  }

  closeDialog() {
    if (this.dialogSettings.onCloseFunction) {
      this.dialogSettings.onCloseFunction(this.dialogContent);
    }
    this.dialogSettings.toggleModal();
    this.close.emit({});
  }

  sendAction() {
    let resp = this.dialogContent.prepare();
    if (resp.success) {
      this.dialogSettings.actionFunction();
    } else {
      this.dialogSettings.setMessage(`There was a problem preparing ${this.dialogContent.getName()}: ${resp.message}`);
    }
    this.dialogSettings.dialogForm = false;
    this.dialogSettings.dialogLog = true;
  }
}
