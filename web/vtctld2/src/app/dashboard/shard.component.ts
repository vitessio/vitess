import { ActivatedRoute, Router } from '@angular/router';
import { Component, OnDestroy, OnInit } from '@angular/core';

import { Observable } from 'rxjs/Observable';

import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { DeleteShardFlags, InitShardMasterFlags, ValidateShardFlags } from '../shared/flags/shard.flags';
import { DeleteShardFlags, InitShardMasterFlags, ValidateShardFlags, TabExtRepFlags,
         PlanRepShardFlags, EmergencyRepShardFlags, ShardReplicationPosFlags, ValidateVerShardFlags } from '../shared/flags/shard.flags';
import { KeyspaceService } from '../api/keyspace.service';
import { TabletService } from '../api/tablet.service';
import { VtctlService } from '../api/vtctl.service';

@Component({
  selector: 'vt-shard-view',
  templateUrl: './shard.component.html',
  styleUrls: [
    './shard.component.css',
    '../styles/vt.style.css'
  ],
  ],
})
export class ShardComponent implements OnInit, OnDestroy {
  private routeSub: any;
  keyspaceName: string;
  shardName: string;
  keyspace = {};
  tablets = [];
  tabletsReady = false;
  // Bound to the table of tablets and must start with none selected.
  selectedTablet = { label: ''};
  dialogSettings: DialogSettings;
  dialogContent: DialogContent;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private keyspaceService: KeyspaceService,
    private tabletService: TabletService,
    private vtctlService: VtctlService) {}

  ngOnInit() {
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();

    this.routeSub = this.route.queryParams.subscribe(params => {
      let keyspaceName = params['keyspace'];
      let shardName = params['shard'];
      if (keyspaceName && shardName) {
        this.keyspaceName = keyspaceName;
        this.shardName = shardName;
        this.getKeyspace(this.keyspaceName);
      }
    });
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
  }

  getTabletList(keyspaceName: string, shardName: string) {
    this.tablets = [];
    this.tabletsReady = true;
    let shardRef = `${keyspaceName}/${shardName}`;
    this.tabletService.getTabletList(shardRef).subscribe((tabletList) => {
      for (let tablet of tabletList){
        this.getTablet(`${tablet.cell}-${tablet.uid}`);
      }
    });
  }

  getTablet(tabletRef: string) {
    this.tabletService.getTablet(tabletRef).subscribe((tablet) => {
      this.tablets.push(tablet);
    });
  }

  getKeyspace(keyspaceName: string) {
    this.keyspaceService.getKeyspace(keyspaceName).subscribe(keyspaceStream => {
      keyspaceStream.subscribe(keyspace => {
        this.keyspace = keyspace;
        // Test to see if shard is in keyspace
        for (let shard of this.keyspace['servingShards'].concat(this.keyspace['nonservingShards'])){
          if (shard === this.shardName) {
            this.getTabletList(this.keyspaceName, this.shardName);
            return;
          }
        }
      });
    });
  }

  deleteShard() {
    this.runCommand('There was a problem deleting {{shard_ref}}:');
  }

  validateShard() {
    this.runCommand('There was a problem validating {{shard_ref}}:');
  }

  initShardMaster() {
    this.runCommand('There was a problem initializing a shard master for {{shard_ref}}:');
  }

  TabletExternallyReparented() {
    this.runCommand(`There was a problem there was a problem updating {{tablet_alias}}'s metatdata:`);
  }

  PlanRepShard() {
    this.runCommand('There was a problem initializing a shard master for {{shard_ref}}:');
  }

  EmergencyRepShard() {
    this.runCommand('There was a problem initializing a shard master for {{shard_ref}}:');
  }

  ShardReplicationPos() {
    this.runCommand('There was a problem initializing a shard master for {{shard_ref}}:');
  }

  ValidateVerShard() {
    this.runCommand('There was a problem initializing a shard master for {{shard_ref}}:');
  }

  runCommand(errorMessage: string) {
    this.dialogSettings.startPending();
    this.vtctlService.runCommand(this.dialogContent.getPostBody()).subscribe(resp => {
      if (resp.Error) {
        this.dialogSettings.setMessage(`${errorMessage} ${resp.Error}`);
      }
      this.dialogSettings.setLog(resp.Output);
      this.dialogSettings.endPending();
    });
  }

  openDeleteShardDialog() {
    this.dialogSettings = new DialogSettings('Delete', this.deleteShard.bind(this),
                                             `Delete ${this.shardName}`, `Are you sure you want to delete ${this.shardName}?`);
    this.dialogSettings.setMessage('Deleted {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.navigateToKeyspace.bind(this);
    let flags = new DeleteShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'DeleteShard');
    this.dialogSettings.toggleModal();
  }

  openValidateShardDialog() {
    this.dialogSettings = new DialogSettings('Validate', this.validateShard.bind(this), `Validate ${this.shardName}`, '');
    this.dialogSettings.setMessage('Validated {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new ValidateShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ValidateShard');
    this.dialogSettings.toggleModal();
  }

  openInitShardMasterDialog() {
    this.dialogSettings = new DialogSettings('Initialize', this.initShardMaster.bind(this), `Initialize ${this.shardName} Master`, '');
    this.dialogSettings.setMessage('Initialized shard master for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new InitShardMasterFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'InitShardMaster');
    this.dialogSettings.toggleModal();
  }

  refreshShardView() {
    this.getKeyspace(this.keyspaceName);
  }

  openTabExtRepDialog() {
    this.dialogSettings = new DialogSettings('Update', this.TabletExternallyReparented.bind(this), `Externally Reparent a Tablet`);
    this.dialogSettings.setMessage('Changed metadata in the topology server for {{tablet_alias}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new TabExtRepFlags(this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'TabletExternallyReparented');
    this.dialogSettings.toggleModal();
  }

  openPlanRepShardDialog() {
    this.dialogSettings = new DialogSettings('Reparent', this.PlanRepShard.bind(this), `Plan to reparent a shard`, '');
    this.dialogSettings.setMessage('Reparented {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PlanRepShardFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'PlannedReparentShard');
    this.dialogSettings.toggleModal();
  }

  openEmergencyRepShardDialog() {
    this.dialogSettings = new DialogSettings('Reparent', this.EmergencyRepShard.bind(this), `Emergency Reparent Shard`);
    this.dialogSettings.setMessage('Initialized shard master for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new EmergencyRepShardFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'EmergencyReparentShard');
    this.dialogSettings.toggleModal();
  }

  openShardReplicationPosDialog() {
    this.dialogSettings = new DialogSettings('Get', this.ShardReplicationPos.bind(this), `Get ${this.shardName} Replication Positions`);
    this.dialogSettings.setMessage('Fetched Replication Positions for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new ShardReplicationPosFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ShardReplicationPositions');
    this.dialogSettings.toggleModal();
  }

  openValidateVerShardDialog() {
    this.dialogSettings = new DialogSettings('Validate', this.ValidateVerShard.bind(this), `Validate ${this.shardName}'s Version`);
    this.dialogSettings.setMessage(`Validated {{shard_ref}}'s Version`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new ValidateVerShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ValidateVersionShard');
    this.dialogSettings.toggleModal();
  }

  navigateToKeyspace(dialogContent: DialogContent) {
    this.router.navigate(['/keyspace'], {queryParams: {keyspace: this.keyspaceName}});
  }

  navigate(tablet) {
    this.router.navigate(['/tablet'], {queryParams: {keyspace: this.keyspaceName, shard: this.shardName, tablet: tablet.ref}});
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
