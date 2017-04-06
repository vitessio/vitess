import { Component, OnInit} from '@angular/core';
import { Router } from '@angular/router';

import { Observable } from 'rxjs/Observable';

import { DeleteKeyspaceFlags, EditKeyspaceFlags, NewKeyspaceFlags, ValidateAllFlags } from '../shared/flags/keyspace.flags';
import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { FeaturesService } from '../api/features.service';
import { Keyspace } from '../api/keyspace';
import { KeyspaceService } from '../api/keyspace.service';
import { VtctlService } from '../api/vtctl.service';
import { PrepareResponse } from '../shared/prepare-response';

import { MenuItem } from 'primeng/primeng';

@Component({
  selector: 'vt-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['../styles/vt.style.css', './dashboard.component.css'],
})

export class DashboardComponent implements OnInit {
  keyspaces = [];
  inFlightQueries = 0;
  selectedKeyspace: Keyspace;
  dialogSettings: DialogSettings;
  dialogContent: DialogContent;
  private actions: MenuItem[];
  private actionsCrud: MenuItem[];
  private keyspaceActions: MenuItem[];

  constructor(
    private featuresService: FeaturesService,
    private keyspaceService: KeyspaceService,
    private router: Router,
    private vtctlService: VtctlService) {}

  ngOnInit() {
    this.getKeyspaces();
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();
    this.actions = [
      {label: 'Validate', command: (event) => {this.openValidateDialog(); }},
    ];
    this.actionsCrud = [
      {label: 'Status', items: [
        {label: 'Validate', command: (event) => {this.openValidateDialog(); }},
      ]},
      {label: 'Change', items: [
        {label: 'New', command: (event) => {this.openNewDialog(); }},
      ]},
    ];
    this.keyspaceActions = [
      {label: 'Change', items: [
        {label: 'Edit', command: (event) => {this.openEditDialog(); }},
        {label: 'Delete', command: (event) => {this.openDeleteDialog(); }},
      ]},
    ];
  }

  getKeyspaces() {
    if (this.inFlightQueries > 0) {
      // There is already a query in flight, we don't want to have
      // more than one at a time, it would mess up our data
      // structures.
      return;
    }
    this.keyspaces = [];
    this.inFlightQueries++;
    this.keyspaceService.getKeyspaces().subscribe(keyspaceStream => {
      this.inFlightQueries++;
      keyspaceStream.subscribe(keyspace => {
          this.keyspaces.push(keyspace);
          this.keyspaces.sort(this.cmp);
      }, () => {
        console.log('Error getting keyspace');
        this.inFlightQueries--;
      }, () => {
        this.inFlightQueries--;
      });
    }, () => {
      console.log('Error getting keyspaces');
      this.inFlightQueries--;
    }, () => {
      this.inFlightQueries--;
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

  setSelectedKeyspace(keyspace) {
    this.selectedKeyspace = keyspace;
  }

  openEditDialog(keyspace = this.selectedKeyspace) {
    this.dialogSettings = new DialogSettings('Edit', `Edit ${keyspace.name}`, '', 'There was a problem editing {{keyspace_name}}:');
    this.dialogSettings.setMessage('Edited {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new EditKeyspaceFlags(keyspace).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, this.prepareEdit.bind(this), 'SetKeyspaceShardingInfo');
    this.dialogSettings.toggleModal();
  }

  openNewDialog() {
    this.dialogSettings = new DialogSettings('Create', 'Create a new Keyspace', '', 'There was a problem creating {{keyspace_name}}:');
    this.dialogSettings.setMessage('Created {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new NewKeyspaceFlags().flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {'keyspace_name': true}, this.prepareNew.bind(this), 'CreateKeyspace');
    this.dialogSettings.toggleModal();
  }

  openDeleteDialog(keyspace = this.selectedKeyspace) {
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
    this.dialogSettings.setMessage('Validated');
    this.dialogSettings.onCloseFunction = this.getKeyspaces.bind(this);
    let flags = new ValidateAllFlags().flags;
    this.dialogContent = new DialogContent('', flags, {}, undefined, 'Validate');
    this.dialogSettings.toggleModal();
  }

  prepareNew(flags) {
    let newFlags = new NewKeyspaceFlags().flags;
    for (let key of Object.keys(flags)) {
      newFlags[key].value = flags[key].value;
    }
    this.shardColumnAndNameSanitize(newFlags);
    return new PrepareResponse(true, newFlags);
  }

  prepareEdit(flags) {
    let newFlags = new EditKeyspaceFlags({name: '', shardingColumnName: '', shardingColumnType: ''}).flags;
    for (let key of Object.keys(flags)) {
      newFlags[key].value = flags[key].value;
    }
    this.shardColumnAndNameSanitize(newFlags);
    return new PrepareResponse(true, newFlags);
  }

  shardColumnAndNameSanitize(newFlags) {
    if (!newFlags['sharding_column_name'].value) {
      newFlags['sharding_column_type']['value'] = '';
    }
    if (newFlags['sharding_column_name'].value && !newFlags['sharding_column_type'].value) {
      newFlags['sharding_column_type']['value'] = 'UINT64';
    }
  }

  navigate(keyspaceName: string) {
    this.router.navigate(['/keyspace'], {queryParams: { keyspace: keyspaceName }});
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
