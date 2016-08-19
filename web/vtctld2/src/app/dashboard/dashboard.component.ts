import { Component, OnInit} from '@angular/core';
import { Router, ROUTER_DIRECTIVES } from '@angular/router';

import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_CHECKBOX_DIRECTIVES } from '@angular2-material/checkbox';
import { MD_INPUT_DIRECTIVES } from '@angular2-material/input';
import { MD_LIST_DIRECTIVES } from '@angular2-material/list/list';
import { MD_PROGRESS_BAR_DIRECTIVES } from '@angular2-material/progress-bar';

import { Observable } from 'rxjs/Observable';

import { AddButtonComponent } from '../shared/add-button.component';
import { DialogComponent } from '../shared/dialog/dialog.component';
import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { ForceFlag, KeyspaceNameFlag, RecursiveFlag, ShardingColumnNameFlag, ShardingColumnTypeFlag } from '../shared/flags/keyspace.flags';
import { KeyspaceService } from '../api/keyspace.service';
import { PrepareResponse } from '../shared/prepare-response';
import { Proto } from '../shared/proto';
import { ShardService } from '../api/shard.service';

@Component({
  selector: 'vt-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['../styles/vt.style.css'],
  providers: [
    KeyspaceService,
    ShardService
  ],
  directives: [
    ROUTER_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    MD_PROGRESS_BAR_DIRECTIVES,
    MD_CHECKBOX_DIRECTIVES,
    MD_INPUT_DIRECTIVES,
    MD_BUTTON_DIRECTIVES,
    MD_LIST_DIRECTIVES,
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
              private router: Router) {}

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

  cmp(a: DialogContent, b: DialogContent): number {
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
    this.dialogSettings.startPending();
    this.keyspaceService.createKeyspace(this.dialogContent).subscribe(resp => {
      if (resp.Error) {
        this.dialogSettings.setMessage('There was a problem creating ', ': ' + resp.Output);
      } else {
        this.getKeyspaces();
      }
      this.dialogSettings.endPending();
    });
  }

  editKeyspace() {
    this.dialogSettings.startPending();
    this.keyspaceService.editKeyspace(this.dialogContent).subscribe(resp => {
      if (resp.Error) {
        this.dialogSettings.setMessage('There was a problem editing ', ': ' + resp.Output);
      } else {
        this.getKeyspaces();
      }
      this.dialogSettings.endPending();
    });
  }

  deleteKeyspace() {
    this.dialogSettings.startPending();
    this.keyspaceService.deleteKeyspace(this.dialogContent).subscribe(resp => {
      if (resp.Error) {
        this.dialogSettings.setMessage('There was a problem deleting ', ': ' + resp.Output);
      } else {
        this.getKeyspaces();
      }
      this.dialogSettings.endPending();
    });
  }

  /*
    Opens/closes the gray modal behind a dialog box.
  */
  toggleModal() {
    this.dialogSettings.openModal = !this.dialogSettings.openModal;
  }

  prepareEdit(keyspace: any) {
    this.dialogSettings = new DialogSettings('Edit', this.editKeyspace.bind(this), `Edit ${keyspace.name}`);
    this.dialogSettings.setMessage('Edited', '');
    let flags = [];
    flags['shardingColumnName'] = new ShardingColumnNameFlag(0, 'shardingColumnName', keyspace.shardingColumnName);
    let shardingColumnType = Proto.SHARDING_COLUMN_NAMES[keyspace.shardingColumnType];
    flags['shardingColumnType'] = new ShardingColumnTypeFlag(1, 'shardingColumnType', shardingColumnType);
    flags['force'] = new ForceFlag(2, 'force');
    this.dialogContent = new DialogContent(keyspace.name, flags, {}, this.prepare.bind(this));
  }

  prepareNew() {
    this.dialogSettings = new DialogSettings('Create', this.createKeyspace.bind(this), 'Create a new Keyspace');
    this.dialogSettings.setMessage('Created', '');
    let flags = {};
    flags['keyspaceName'] = new KeyspaceNameFlag(0, 'keyspaceName');
    flags['shardingColumnName'] = new ShardingColumnNameFlag(1, 'shardingColumnName');
    flags['shardingColumnType'] = new ShardingColumnTypeFlag(2, 'shardingColumnType');
    flags['force'] = new ForceFlag(3, 'force');
    this.dialogContent = new DialogContent('', flags, {'keyspaceName': true}, this.prepare.bind(this));
  }

  prepareDelete(keyspace: any) {
    this.dialogSettings = new DialogSettings('Delete', this.deleteKeyspace.bind(this),
                                             `Delete ${keyspace.name}`, `Are you sure you want to delete ${keyspace.name} ?`);
    this.dialogSettings.setMessage('Deleted', '');
    let flags = {};
    flags['recursive'] = new RecursiveFlag(0, 'recursive');
    this.dialogContent = new DialogContent(keyspace.name, flags);
  }

  blockClicks(event: any) {
    event.stopPropagation();
  }

  navigate(keyspaceName: string) {
    this.router.navigate(['/keyspace'], {queryParams: { keyspace: keyspaceName }});
  }

  prepare(flags) {
    if (flags['shardingColumnType'].getStrValue() !== '') {
      flags['shardingColumnType'].setValue(Proto.SHARDING_COLUMN_NAME_TO_TYPE[flags['shardingColumnType'].getStrValue()]);
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
