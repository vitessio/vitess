import { Component, OnInit} from '@angular/core';
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

  openEditDialog(keyspace: Keyspace) {
    this.dialogSettings = new DialogSettings('Edit', `Edit ${keyspace.name}`, '', 'There was a problem editing {{keyspace_name}}:');
    this.dialogSettings.setMessage('Edited {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new EditKeyspaceFlags(keyspace).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined, 'SetKeyspaceShardingInfo');
    this.dialogSettings.toggleModal();
  }

  openNewDialog() {
    this.dialogSettings = new DialogSettings('Create', 'Create a new Keyspace', '', 'There was a problem creating {{keyspace_name}}:');
    this.dialogSettings.setMessage('Created {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new NewKeyspaceFlags().flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {'keyspace_name': true}, undefined, 'CreateKeyspace');
    this.dialogSettings.toggleModal();
  }

  openDeleteDialog(keyspace: Keyspace) {
    this.dialogSettings = new DialogSettings('Delete', `Delete ${keyspace.name}`, `Are you sure you want to delete ${keyspace.name}?`,
                                             'There was a problem deleting {{keyspace_name}}:');
    this.dialogSettings.setMessage('Deleted {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new DeleteKeyspaceFlags(keyspace.name).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined, 'DeleteKeyspace');
    this.dialogSettings.toggleModal();
  }

  openValidateDialog() {
    this.dialogSettings = new DialogSettings('Validate', `Validate all nodes`, '', 'There was a problem validating all nodes:');
    this.dialogSettings.setMessage('Deleted {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new ValidateAllFlags().flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined, 'Validate');
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
