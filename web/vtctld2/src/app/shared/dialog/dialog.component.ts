import { Component, EventEmitter, Input, Output } from '@angular/core';

import { DialogContent } from './dialog-content';
import { DialogSettings } from './dialog-settings';

import { VtctlService } from '../../api/vtctl.service';

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

  constructor(private vtctlService: VtctlService) {}

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
      this.runCommand();
    } else {
      this.dialogSettings.setMessage(`There was a problem preparing ${this.dialogContent.getName()}: ${resp.message}`);
    }
    this.dialogSettings.dialogForm = false;
    this.dialogSettings.dialogLog = true;
  }

  runCommand() {
    this.dialogSettings.startPending();
    this.vtctlService.runCommand(this.dialogContent.getPostBody()).subscribe(resp => {
      if (resp.Error) {
        this.dialogSettings.setMessage(`${this.dialogSettings.errMsg} ${resp.Error}`);
      }
      this.dialogSettings.setLog(resp.Output);
      this.dialogSettings.endPending();
    });
  }

  getCmd() {
    let preppedFlags = this.dialogContent.prepare(false).flags;
    let sortedFlags = this.dialogContent.getFlags(preppedFlags);
    return this.dialogContent.getPostBody(sortedFlags);
  }
}
