import { ActivatedRoute } from '@angular/router';
import { Component, OnInit, OnDestroy } from '@angular/core';

import { Observable } from 'rxjs/Observable';

import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { NewShardFlags } from '../shared/flags/shard.flags';
import { KeyspaceService } from '../api/keyspace.service';
import { PrepareResponse } from '../shared/prepare-response';
import { RebuildKeyspaceGraphFlags, ReloadSchemaKeyspaceFlags,
         RemoveKeyspaceCellFlags, ValidateKeyspaceFlags,
         ValidateSchemaFlags, ValidateVersionFlags } from '../shared/flags/keyspace.flags';
import { VtctlService } from '../api/vtctl.service';

import { MenuItem } from 'primeng/primeng';

@Component({
  selector: 'vt-keyspace-view',
  templateUrl: './keyspace.component.html',
  styleUrls: ['../styles/vt.style.css'],
})

export class KeyspaceComponent implements OnInit, OnDestroy {

  private routeSub: any;
  keyspaceName: string;
  inFlightQueries = 0;
  keyspace = {};
  dialogSettings: DialogSettings;
  dialogContent: DialogContent;
  private actions: MenuItem[];

  constructor(
    private route: ActivatedRoute,
    private keyspaceService: KeyspaceService,
    private vtctlService: VtctlService) {}

  ngOnInit() {
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();
    this.actions = [
      {label: 'Status', items: [
        {label: 'Validate', command: (event) => {this.openValidateKeyspaceDialog(); }},
        {label: 'Validate Schema', command: (event) => {this.openValidateSchemaDialog(); }},
        {label: 'Validate Version', command: (event) => {this.openValidateVersionDialog(); }},
      ]},
      {label: 'Rebuild / Reload', items: [
        {label: 'Rebuild Keyspace Graph', command: (event) => {this.openRebuildKeyspaceGraphDialog(); }},
        {label: 'Reload Schema in Keyspace', command: (event) => {this.openReloadSchemaKeyspaceDialog(); }},
      ]},
      {label: 'Change', items: [
        {label: 'Remove Keyspace Cell', command: (event) => {this.openRemoveKeyspaceCellDialog(); }},
        {label: 'New Shard', command: (event) => {this.openNewShardDialog(); }},
      ]},
    ];

    this.routeSub = this.route.queryParams.subscribe(params => {
      let keyspaceName = params['keyspace'];
      if (keyspaceName) {
        this.keyspaceName = keyspaceName;
        this.getKeyspace(this.keyspaceName);
      }
    });
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
  }

  getKeyspace(keyspaceName: string) {
    if (this.inFlightQueries > 0) {
      // There is already a query in flight, we don't want to have
      // more than one at a time, it would mess up our data
      // structures.
      return;
    }
    this.inFlightQueries++;
    this.keyspaceService.getKeyspace(keyspaceName).subscribe(keyspaceStream => {
      this.inFlightQueries++;
      keyspaceStream.subscribe(keyspace => {
          this.keyspace = keyspace;
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

  openNewShardDialog() {
    this.dialogSettings = new DialogSettings('Create', 'Create a new Shard', '',
                                             'There was a problem creating {{shard_ref}}:');
    this.dialogSettings.setMessage('Created {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshKeyspaceView.bind(this);
    let flags = new NewShardFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('shard_ref', flags, {}, this.prepareShard.bind(this));
    this.dialogContent = new DialogContent('shard_ref', flags, {}, this.prepareShard.bind(this), 'CreateShard');
    this.dialogSettings.toggleModal();
  }

  openValidateKeyspaceDialog() {
    this.dialogSettings = new DialogSettings('Validate', `Validate ${this.keyspaceName}`, '',
                                             'There was a problem validating {{keyspace_name}}:');
    this.dialogSettings.setMessage('Validated {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.refreshKeyspaceView.bind(this);
    let flags = new ValidateKeyspaceFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined, 'ValidateKeyspace');
    this.dialogSettings.toggleModal();
  }

  openValidateSchemaDialog() {
    this.dialogSettings = new DialogSettings('Validate', `Validate ${this.keyspaceName}'s Schema`, '',
                                             `There was a problem validating {{keyspace_name}}'s Schema:`);
    this.dialogSettings.setMessage(`Validated {{keyspace_name}}'s Schema`);
    this.dialogSettings.onCloseFunction = this.refreshKeyspaceView.bind(this);
    let flags = new ValidateSchemaFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined, 'ValidateSchemaKeyspace');
    this.dialogSettings.toggleModal();
  }

  openValidateVersionDialog() {
    this.dialogSettings = new DialogSettings('Validate', `Validate ${this.keyspaceName}'s Version`, '',
                                             `There was a problem validating {{keyspace_name}}'s Version:`);
    this.dialogSettings.setMessage(`Validated {{keyspace_name}}'s Version`);
    this.dialogSettings.onCloseFunction = this.refreshKeyspaceView.bind(this);
    let flags = new ValidateVersionFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined, 'ValidateVersionKeyspace');
    this.dialogSettings.toggleModal();
  }

  openRebuildKeyspaceGraphDialog() {
    this.dialogSettings = new DialogSettings('Rebuild', `Rebuild ${this.keyspaceName}`, '',
                                             'There was a problem rebuilding {{keyspace_name}}:');
    this.dialogSettings.setMessage('Rebuilt {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.refreshKeyspaceView.bind(this);
    let flags = new RebuildKeyspaceGraphFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined, 'RebuildKeyspaceGraph');
    this.dialogSettings.toggleModal();
  }

  openReloadSchemaKeyspaceDialog() {
    this.dialogSettings = new DialogSettings('Reload Schema', `Reload Schema in ${this.keyspaceName}`, '',
                                             'There was a problem reloading schema in {{keyspace_name}}:');
    this.dialogSettings.setMessage('Reloaded {{keyspace_name}} Schema');
    let flags = new ReloadSchemaKeyspaceFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {}, undefined, 'ReloadSchemaKeyspace');
    this.dialogSettings.toggleModal();
  }

  openRemoveKeyspaceCellDialog() {
    this.dialogSettings = new DialogSettings('Remove', `Remove a cell from ${this.keyspaceName}`, '',
                                             'There was a problem removing {{cell_name}}:');
    this.dialogSettings.setMessage('Removed {{cell_name}}');
    this.dialogSettings.onCloseFunction = this.refreshKeyspaceView.bind(this);
    let flags = new RemoveKeyspaceCellFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('cell_name', flags, {}, undefined, 'RemoveKeyspaceCell');
    this.dialogSettings.toggleModal();
  }

  refreshKeyspaceView() {
    this.getKeyspace(this.keyspaceName);
  }

  /*
    Creates a shard reference from the keyspacename paired with the lower and
    upper bounds. Sets all other flag values to the empty string so they don't
    end up in the request.
  */
  prepareShard(flags: any) {
    let newFlags = {};
    newFlags['shard_ref'] = flags['shard_ref'];
    let shardName = this.getName(flags['lower_bound'].getStrValue(), flags['upper_bound'].getStrValue());
    newFlags['shard_ref'].setValue(flags['keyspace_name'].getStrValue() + '/' + shardName);
    return new PrepareResponse(true, newFlags);
  }

  // Functions for parsing shardName
  getName(lowerBound: string, upperBound: string) {
    this.dialogContent.setName(lowerBound + '-' + upperBound);
    return this.dialogContent.getName();
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }

  noShards() {
    if (this.keyspace === undefined) {
      return false;
    }
    if (this.keyspace['servingShards'] === undefined || this.keyspace['nonservingShards'] === undefined) {
      return false;
    }
    if (this.keyspace['servingShards'].length === 0 && this.keyspace['nonservingShards'].length === 0) {
      return true;
    }
    return false;
  }
}
