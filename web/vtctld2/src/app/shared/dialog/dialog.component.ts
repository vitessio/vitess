import { Component, ComponentResolver, EventEmitter, Input, AfterViewInit, Output, ViewChild, ViewContainerRef} from '@angular/core';
import { ROUTER_DIRECTIVES } from '@angular/router';

import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_CHECKBOX_DIRECTIVES } from '@angular2-material/checkbox';
import { MD_INPUT_DIRECTIVES } from '@angular2-material/input';
import { MD_PROGRESS_BAR_DIRECTIVES } from '@angular2-material/progress-bar';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';

import { DialogContent } from './dialog-content';
import { DialogSettings } from './dialog-settings';
import { KeyspaceService } from '../../api/keyspace.service';
import { TabletService } from '../../api/tablet.service';

import {Dropdown} from 'primeng/primeng';

@Component({
  selector: 'vt-dialog',
  templateUrl: './dialog.component.html',
  styleUrls: ['./dialog.component.css', '../../styles/vt.style.css'],
  providers: [
    KeyspaceService,
    TabletService
  ],
  directives: [
    Dropdown,
    ROUTER_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    MD_PROGRESS_BAR_DIRECTIVES,
    MD_CHECKBOX_DIRECTIVES,
    MD_INPUT_DIRECTIVES,
    MD_BUTTON_DIRECTIVES,
  ],
})
export class DialogComponent implements AfterViewInit {
  title = 'Vitess Control Panel';
  keyspaces = [];
  extraContentReference: any;
  @Input() dialogContent: DialogContent;
  @Input() dialogSettings: DialogSettings;
  @Input() dialogExtraContent: any;
  @Output() close = new EventEmitter();
  @ViewChild('vtFormWrapper', {read: ViewContainerRef}) vtFormWrapper: any;

  constructor(private componentResolver: ComponentResolver) {}

  ngAfterViewInit() {
    if (this.dialogExtraContent) {
      this.componentResolver.resolveComponent(this.dialogExtraContent).then(factory => {
        this.extraContentReference = this.vtFormWrapper.createComponent(factory, 0);
      });
    }
  }

  loadExtraContent(dialogContent) {
    if (this.dialogExtraContent) {
      this.extraContentReference.instance.dialogContent = dialogContent;
    }
  }

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

  getCmd() {
    let preppedFlags = this.dialogContent.prepare(false).flags;
    let sortedFlags = this.dialogContent.getFlags(preppedFlags);
    return this.dialogContent.getPostBody(sortedFlags);
  }
  logToArray(logText) {
    return logText.split('\n');
  }
}
