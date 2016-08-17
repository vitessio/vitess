import { Component, OnInit} from '@angular/core';
import { Router, ROUTER_DIRECTIVES } from '@angular/router';

import { Observable } from 'rxjs/Observable';

import { AddButtonComponent } from '../shared/add-button.component';
import { DeleteKeyspaceFlags, EditKeyspaceFlags, NewKeyspaceFlags, ValidateAllFlags } from '../shared/flags/keyspace.flags';
import { DialogComponent } from '../shared/dialog/dialog.component';
import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { Keyspace } from '../api/keyspace';
import { KeyspaceService } from '../api/keyspace.service';
import { PrepareResponse } from '../shared/prepare-response';
import { Proto } from '../shared/proto';
import { ShardService } from '../api/shard.service';
import { VtctlService } from '../api/vtctl.service';

@Component({
  moduleId: module.id,
  selector: 'vt-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['../styles/vt.style.css'],
  providers: [
    KeyspaceService,
    ShardService,
    VtctlService
  ],
  directives: [
    ROUTER_DIRECTIVES,
    DialogComponent,
    AddButtonComponent],
})

export class DashboardComponent implements OnInit {
  title = 'Vitess Control Panel';
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
    this.serverCall('There was a problem creating {{keyspace_name}}:');
  }

  editKeyspace() {
    this.serverCall('There was a problem editing {{keyspace_name}}:');
  }

  deleteKeyspace() {
    this.serverCall('There was a problem deleting {{keyspace_name}}:');
  }

  validateAll() {
    this.serverCall('There was a problem validating all nodes:');
  }

  serverCall(errorMessage: string) {
    this.vtctlService.serverCall('', this.dialogContent, this.dialogSettings, errorMessage);
  }

  prepareEdit(keyspace: Keyspace) {
    this.dialogSettings = new DialogSettings('Edit', this.editKeyspace.bind(this), `Edit ${keyspace.name}`);
    this.dialogSettings.setMessage('Edited {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.refreshDashboardView.bind(this);
    let flags = new EditKeyspaceFlags(keyspace).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, this.prepare.bind(this), 'SetKeyspaceShardingInfo');
    this.dialogSettings.toggleModal();
  }

  prepareNew() {
    this.dialogSettings = new DialogSettings('Create', this.createKeyspace.bind(this), 'Create a new Keyspace');
    this.dialogSettings.setMessage('Created {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.refreshDashboardView.bind(this);
    let flags = new NewKeyspaceFlags().flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {'keyspace_name': true}, undefined, 'CreateKeyspace');
    this.dialogSettings.toggleModal();
  }

  prepareDelete(keyspace: Keyspace) {
    this.dialogSettings = new DialogSettings('Delete', this.deleteKeyspace.bind(this),
                                             `Delete ${keyspace.name}`, `Are you sure you want to delete ${keyspace.name}?`);
    this.dialogSettings.setMessage('Deleted {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.refreshDashboardView.bind(this);
    let flags = new DeleteKeyspaceFlags(keyspace.name).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined, 'DeleteKeyspace');
    this.dialogSettings.toggleModal();
  }

  prepareValidate() {
    this.dialogSettings = new DialogSettings('Validate', this.validateAll.bind(this), `Validate all nodes`, '');
    this.dialogSettings.setMessage('Deleted {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.refreshDashboardView.bind(this);
    let flags = new ValidateAllFlags().flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined, 'Validate');
    this.dialogSettings.toggleModal();
  }

  refreshDashboardView() {
    this.getKeyspaces();
  }

  navigate(keyspaceName: string) {
    this.router.navigate(['/keyspace'], {queryParams: { keyspace: keyspaceName }});
  }

  prepare(flags) {
    if (flags['sharding_column_type'].getStrValue() !== '') {
      flags['sharding_column_type'].setValue(Proto.SHARDING_COLUMN_NAME_TO_TYPE[flags['sharding_column_type'].getStrValue()]);
    }
    return new PrepareResponse(true, flags);
  }

  logView() {
    this.dialogSettings.dialogForm = false;
    this.dialogSettings.dialogLog = true;
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
