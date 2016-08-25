import { Component, OnInit, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { Observable } from 'rxjs/Observable';

import { DeleteKeyspaceFlags, EditKeyspaceFlags, NewKeyspaceFlags, ValidateAllFlags } from '../shared/flags/keyspace.flags';
import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { Keyspace } from '../api/keyspace';
import { KeyspaceService } from '../api/keyspace.service';
import { VtctlService } from '../api/vtctl.service';

@Component({
  selector: 'vt-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['../styles/vt.style.css'],
})
export class DashboardComponent implements OnInit {
  @ViewChild('dialog') dialog: any;
  keyspaces = [];
  keyspacesReady = false;
  dialogSettings: DialogSettings;
  dialogContent: DialogContent;

  constructor(
              private keyspaceService: KeyspaceService,
              private router: Router,
              private vtctlService: VtctlService) {}

  ngOnInit() {
    this.getKeyspaces();
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();
  }

  getKeyspaces() {
    this.keyspaces = [];
    this.keyspaceService.getKeyspaces().subscribe(keyspaceStream => {
      keyspaceStream.subscribe(keyspace => {
          this.keyspaces.push(keyspace);
          this.keyspaces.sort(this.cmp);
      });
    });
  }

  cmp(a: Keyspace, b: Keyspace): number {
    let aLowercase = a.name.toLowerCase();
    let bLowercase = b.name.toLowerCase();
    if (aLowercase > bLowercase) {
      return 1;
    }
    if (bLowercase > aLowercase) {
      return -1;
    }
    return 0;
  }

  createKeyspace() {
    this.runCommand('CreateKeyspace', 'There was a problem creating {{keyspace_name}}:');
  }

  editKeyspace() {
    this.runCommand('SetKeyspaceShardingInfo', 'There was a problem editing {{keyspace_name}}:');
  }

  deleteKeyspace() {
    this.runCommand('DeleteKeyspace', 'There was a problem deleting {{keyspace_name}}:');
  }

  validateAll() {
    this.runCommand('Validate', 'There was a problem validating all nodes:');
  }

  runCommand(action: string, errorMessage: string) {
    this.dialogSettings.startPending();
    this.vtctlService.runCommand(this.dialogContent.getPostBody(action)).subscribe(resp => {
      if (resp.Error) {
        this.dialogSettings.setMessage(`${errorMessage} ${resp.Error}`);
      }
      this.dialogSettings.setLog(resp.Output);
      this.dialogSettings.endPending();
    });
  }

  openEditDialog(keyspace: Keyspace) {
    this.dialogSettings = new DialogSettings('Edit', this.editKeyspace.bind(this), `Edit ${keyspace.name}`);
    this.dialogSettings.setMessage('Edited {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new EditKeyspaceFlags(keyspace).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined);
    this.dialogSettings.toggleModal();
  }

  openNewDialog() {
    this.dialogSettings = new DialogSettings('Create', this.createKeyspace.bind(this), 'Create a new Keyspace');
    this.dialogSettings.setMessage('Created {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new NewKeyspaceFlags().flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {'keyspace_name': true}, undefined);
    this.dialogSettings.toggleModal();
  }

  openDeleteDialog(keyspace: Keyspace) {
    this.dialogSettings = new DialogSettings('Delete', this.deleteKeyspace.bind(this),
                                             `Delete ${keyspace.name}`, `Are you sure you want to delete ${keyspace.name}?`);
    this.dialogSettings.setMessage('Deleted {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new DeleteKeyspaceFlags(keyspace).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags);
    this.dialogSettings.toggleModal();
  }

  openValidateDialog() {
    this.dialogSettings = new DialogSettings('Validate', this.validateAll.bind(this), `Validate all nodes`, '');
    this.dialogSettings.setMessage('Deleted {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new ValidateAllFlags().flags;
    this.dialogContent = new DialogContent('keyspace_name', flags);
    this.dialogSettings.toggleModal();
  }

  navigate(keyspaceName: string) {
    this.router.navigate(['/keyspace'], {queryParams: { keyspace: keyspaceName }});
  }

  logView() {
    this.dialogSettings.dialogForm = false;
    this.dialogSettings.dialogLog = true;
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
