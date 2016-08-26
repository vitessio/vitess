import { ActivatedRoute } from '@angular/router';
import { Component, OnInit, OnDestroy } from '@angular/core';

import { Observable } from 'rxjs/Observable';

import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { NewShardFlags } from '../shared/flags/shard.flags';
import { KeyspaceService } from '../api/keyspace.service';
import { PrepareResponse } from '../shared/prepare-response';
import { RebuildKeyspaceGraphFlags, RemoveKeyspaceCellFlags, ValidateKeyspaceFlags } from '../shared/flags/keyspace.flags';
import { VtctlService } from '../api/vtctl.service';

@Component({
  selector: 'vt-keyspace-view',
  templateUrl: './keyspace.component.html',
  styleUrls: ['../styles/vt.style.css'],
})

export class KeyspaceComponent implements OnInit, OnDestroy {
  private routeSub: any;
  keyspaceName: string;
  shardsReady = false;
  keyspace = {};
  dialogSettings: DialogSettings;
  dialogContent: DialogContent;
  constructor(
    private route: ActivatedRoute,
    private keyspaceService: KeyspaceService,
    private vtctlService: VtctlService) {}

  ngOnInit() {
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();

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

  getKeyspace(keyspaceName) {
    this.keyspaceService.getKeyspace(keyspaceName).subscribe(keyspaceStream => {
      keyspaceStream.subscribe(keyspace => {
          this.keyspace = keyspace;
      });
    });
  }

  createShard() {
    this.runCommand('CreateShard', 'There was a problem creating {{shard_ref}}:');
  }

  validateKeyspace() {
    this.runCommand('ValidateKeyspace', 'There was a problem validating {{keyspace_name}}:');
  }

  rebuildKeyspace() {
    this.runCommand('RebuildKeyspaceGraph', 'There was a problem rebuilding {{keyspace_name}}:');
  }

  removeKeyspaceCell() {
    this.runCommand('RemoveKeyspaceCell', 'There was a problem removing {{cell_name}}:');
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

  openNewShardDialog() {
    this.dialogSettings = new DialogSettings('Create', this.createShard.bind(this), 'Create a new Shard', '');
    this.dialogSettings.setMessage('Created {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshKeyspaceView.bind(this);
    let flags = new NewShardFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('shard_ref', flags, {}, this.prepareShard.bind(this));
    this.dialogSettings.toggleModal();
  }

  openValidateKeyspaceDialog() {
    this.dialogSettings = new DialogSettings('Validate', this.validateKeyspace.bind(this), `Validate ${this.keyspaceName}`, '');
    this.dialogSettings.setMessage('Validated {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.refreshKeyspaceView.bind(this);
    let flags = new ValidateKeyspaceFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {});
    this.dialogSettings.toggleModal();
  }

  openRebuildKeyspaceGraphDialog() {
    this.dialogSettings = new DialogSettings('Rebuild', this.rebuildKeyspace.bind(this), `Rebuild ${this.keyspaceName}`, '');
    this.dialogSettings.setMessage('Rebuilt {{keyspace_name}}');
    this.dialogSettings.onCloseFunction = this.refreshKeyspaceView.bind(this);
    let flags = new RebuildKeyspaceGraphFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('keyspace_name', flags, {});
    this.dialogSettings.toggleModal();
  }

  openRemoveKeyspaceCellDialog() {
    this.dialogSettings = new DialogSettings('Remove', this.removeKeyspaceCell.bind(this), `Remove a cell from ${this.keyspaceName}`, '');
    this.dialogSettings.setMessage('Removed {{cell_name}}');
    this.dialogSettings.onCloseFunction = this.refreshKeyspaceView.bind(this);
    let flags = new RemoveKeyspaceCellFlags(this.keyspaceName).flags;
    this.dialogContent = new DialogContent('cell_name', flags, {});
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
  prepareShard(flags) {
    let shardName = this.getName(flags['lower_bound'].getStrValue(), flags['upper_bound'].getStrValue());
    flags['shard_ref'].setValue(flags['keyspace_name'].getStrValue() + '/' + shardName);
    flags['keyspace_name'].setValue('');
    flags['lower_bound'].setValue('');
    flags['upper_bound'].setValue('');
    return new PrepareResponse(true, flags);
  }

  // Functions for parsing shardName
  getName(lowerBound, upperBound) {
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
