import { ActivatedRoute, Router, ROUTER_DIRECTIVES } from '@angular/router';
import { Component, OnDestroy, OnInit } from '@angular/core';

import { Column, DataTable, Header } from 'primeng/primeng';

import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';

import { Observable } from 'rxjs/Observable';

import { Accordion, AccordionTab } from 'primeng/primeng';

import { AddButtonComponent } from '../shared/add-button.component';
import { DialogComponent } from '../shared/dialog/dialog.component';
import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { DeleteShardFlags, InitShardMasterFlags, ValidateShardFlags, TabExtRepFlags,
         PlanRepShardFlags, EmergencyRepShardFlags, ShardReplicationPosFlags, ValidateVerShardFlags } from '../shared/flags/shard.flags';
import { KeyspaceService } from '../api/keyspace.service';
import { ShardService } from '../api/shard.service';
import { TabletService } from '../api/tablet.service';
import { VtctlService } from '../api/vtctl.service';

@Component({
  selector: 'vt-shard-view',
  templateUrl: './shard.component.html',
  styleUrls: [
    './shard.component.css',
    '../styles/vt.style.css'
  ],
  directives: [
    ROUTER_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    MD_BUTTON_DIRECTIVES,
    DataTable,
    Column,
    AddButtonComponent,
    DialogComponent,
    Header,
    Accordion,
    AccordionTab,
  ],
  providers: [
    KeyspaceService,
    ShardService,
    TabletService,
    VtctlService
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
    let paramStream = this.router.routerState.queryParams;
    let routeStream = this.route.url;
    this.routeSub =  paramStream.combineLatest(routeStream).subscribe( routeData => {
      let params = routeData[0];
      let path = routeData[1][0].path;
      let keyspaceName = params['keyspace'];
      let shardName = params['shard'];
      if (path === 'shard' && keyspaceName && shardName) {
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
    let shardRef = keyspaceName + '/' + shardName;
    this.tabletService.getTabletList(shardRef).subscribe((tabletList) => {
      for (let tablet of tabletList){
        this.getTablet(tablet.cell + '-' + tablet.uid);
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
    this.serverCall('There was a problem deleting {{shard_ref}}:');
  }

  validateShard() {
    this.serverCall('There was a problem validating {{shard_ref}}:');
  }

  initShardMaster() {
    this.serverCall('There was a problem initializing a shard master for {{shard_ref}}:');
  }

  TabletExternallyReparented() {
    this.serverCall(`There was a problem there was a problem updating {{tablet_alias}}'s metatdata:`);
  }

  PlanRepShard() {
    this.serverCall('There was a problem initializing a shard master for {{shard_ref}}:');
  }

  EmergencyRepShard() {
    this.serverCall('There was a problem initializing a shard master for {{shard_ref}}:');
  }

  ShardReplicationPos() {
    this.serverCall('There was a problem initializing a shard master for {{shard_ref}}:');
  }

  ValidateVerShard() {
    this.serverCall('There was a problem initializing a shard master for {{shard_ref}}:');
  }

  serverCall(errorMessage: string) {
    this.vtctlService.serverCall('', this.dialogContent, this.dialogSettings, errorMessage);
  }

  prepareDeleteShard() {
    this.dialogSettings = new DialogSettings('Delete', this.deleteShard.bind(this),
                                             `Delete ${this.shardName}`, `Are you sure you want to delete ${this.shardName}?`);
    this.dialogSettings.setMessage('Deleted {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.navigateToKeyspace.bind(this);
    let flags = new DeleteShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'DeleteShard');
    this.dialogSettings.toggleModal();
  }

  prepareValidateShard() {
    this.dialogSettings = new DialogSettings('Validate', this.validateShard.bind(this), `Validate ${this.shardName}`);
    this.dialogSettings.setMessage('Validated {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new ValidateShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ValidateShard');
    this.dialogSettings.toggleModal();
  }

  prepareInitShardMaster() {
    this.dialogSettings = new DialogSettings('Initialize', this.initShardMaster.bind(this), `Initialize ${this.shardName} Master`);
    this.dialogSettings.setMessage('Initialized shard master for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new InitShardMasterFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'InitShardMaster');
    this.dialogSettings.toggleModal();
  }

  prepareTabExtRep() {
    this.dialogSettings = new DialogSettings('Update', this.TabletExternallyReparented.bind(this), `Externally Reparent a Tablet`);
    this.dialogSettings.setMessage('Changed metadata in the topology server for {{tablet_alias}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new TabExtRepFlags(this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'TabletExternallyReparented');
    this.dialogSettings.toggleModal();
  }

  preparePlanRepShard() {
    this.dialogSettings = new DialogSettings('Reparent', this.PlanRepShard.bind(this), `Plan to reparent a shard`, '');
    this.dialogSettings.setMessage('Reparented {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PlanRepShardFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'PlannedReparentShard');
    this.dialogSettings.toggleModal();
  }

  prepareEmergencyRepShard() {
    this.dialogSettings = new DialogSettings('Reparent', this.EmergencyRepShard.bind(this), `Emergency Reparent Shard`);
    this.dialogSettings.setMessage('Initialized shard master for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new EmergencyRepShardFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'EmergencyReparentShard');
    this.dialogSettings.toggleModal();
  }

  prepareShardReplicationPos() {
    this.dialogSettings = new DialogSettings('Get', this.ShardReplicationPos.bind(this), `Get ${this.shardName} Replication Positions`);
    this.dialogSettings.setMessage('Fetched Replication Positions for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new ShardReplicationPosFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ShardReplicationPositions');
    this.dialogSettings.toggleModal();
  }

  prepareValidateVerShard() {
    this.dialogSettings = new DialogSettings('Validate', this.ValidateVerShard.bind(this), `Validate ${this.shardName}'s Version`);
    this.dialogSettings.setMessage(`Validated {{shard_ref}}'s Version`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new ValidateVerShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ValidateVersionShard');
    this.dialogSettings.toggleModal();
  }

  refreshShardView() {
    this.getKeyspace(this.keyspaceName);
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
