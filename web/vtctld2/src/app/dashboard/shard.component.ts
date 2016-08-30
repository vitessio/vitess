import { ActivatedRoute, Router } from '@angular/router';
import { Component, OnDestroy, OnInit } from '@angular/core';

import { Observable } from 'rxjs/Observable';

import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { DeleteShardFlags, InitShardMasterFlags, ValidateShardFlags, TabExtRepFlags,
         PlanRepShardFlags, EmergencyRepShardFlags, ShardReplicationPosFlags, ValidateVerShardFlags } from '../shared/flags/shard.flags';
import { DeleteTabletFlags, PingTabletFlags, RefreshTabletFlags } from '../shared/flags/tablet.flags';
import { KeyspaceService } from '../api/keyspace.service';
import { TabletService } from '../api/tablet.service';
import { VtctlService } from '../api/vtctl.service';

import { MenuItem } from 'primeng/primeng';

@Component({
  selector: 'vt-shard-view',
  templateUrl: './shard.component.html',
  styleUrls: [
    './shard.component.css',
    '../styles/vt.style.css'
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
  selectedTablet = undefined;
  dialogSettings: DialogSettings;
  dialogContent: DialogContent;
  private actions: MenuItem[];
  private tabletActions: MenuItem[];

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private keyspaceService: KeyspaceService,
    private tabletService: TabletService,
    private vtctlService: VtctlService) {
  }


  ngOnInit() {
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();

    this.actions = [
      {label: 'Delete', command: (event) => {this.openDeleteShardDialog(); }},
      {label: 'Validate', command: (event) => {this.openValidateShardDialog(); }},
      {label: 'Initialize Shard Master', command: (event) => {this.openInitShardMasterDialog(); }},
      {label: 'Externally Reparent Tablet', command: (event) => {this.openTabExtRepDialog(); }},
      {label: 'Planned Reparent', command: (event) => {this.openPlanRepShardDialog(); }},
      {label: 'Emergency Reparent', command: (event) => {this.openEmergencyRepShardDialog(); }},
      {label: 'Shard Replication Positions', command: (event) => {this.openShardReplicationPosDialog(); }},
      {label: 'Validate Version', command: (event) => {this.openValidateVerShardDialog(); }},
    ];
    this.tabletActions = this.getTabletActions();

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

  getTabletActions() {
    return [
      {label: 'Delete', command: (event) => {this.openDeleteTabletDialog(); }},
      {label: 'Ping', command: (event) => {this.openPingTabletDialog(); }},
      {label: 'Refresh State', command: (event) => {this.openRefreshTabletDialog(); }},
      {label: 'Set ReadOnly', command: (event) => {this.openSetReadOnlyDialog(); }},
      {label: 'Set ReadWrite', command: (event) => {this.openSetReadWriteDialog(); }},
      {label: 'Start Slave', command: (event) => {this.openStartSlaveDialog(); }},
      {label: 'Stop Slave', command: (event) => {this.openStopSlaveDialog(); }},
      {label: 'Run Health Check', command: (event) => {this.openRunHealthCheckDialog(); }},
      {label: 'Ignore Health Error', command: (event) => {this.openIgnoreHealthErrorDialog(); }},
      {label: 'Demote Master', command: (event) => {this.openDemoteMasterDialog(); }},
      {label: 'Reparent Tablet', command: (event) => {this.openReparentTabletDialog(); }},
    ];
  }

  portNames(tablet) {
    return Object.keys(tablet.port_map);
  }

  getUrl(tablet) {
    return `http://${tablet.hostname}:${tablet.port_map.vt}`;
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

  openDeleteShardDialog() {
    this.dialogSettings = new DialogSettings('Delete', `Delete ${this.shardName}`,
                                             `Are you sure you want to delete ${this.shardName}?`,
                                             'There was a problem deleting {{shard_ref}}:');
    this.dialogSettings.setMessage('Deleted {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.navigateToKeyspace.bind(this);
    let flags = new DeleteShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'DeleteShard');
    this.dialogSettings.toggleModal();
  }

  openValidateShardDialog() {
    this.dialogSettings = new DialogSettings('Validate', `Validate ${this.shardName}`, '',
                                             'There was a problem validating {{shard_ref}}:');
    this.dialogSettings.setMessage('Validated {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new ValidateShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ValidateShard');
    this.dialogSettings.toggleModal();
  }

  openInitShardMasterDialog() {
    this.dialogSettings = new DialogSettings('Initialize', `Initialize ${this.shardName} Master`, '',
                                             'There was a problem initializing a shard master for {{shard_ref}}:');
    this.dialogSettings.setMessage('Initialized shard master for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new InitShardMasterFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'InitShardMaster');
    this.dialogSettings.toggleModal();
  }

  openTabExtRepDialog() {
    this.dialogSettings = new DialogSettings('Update', `Externally Reparent a Tablet`, '',
                                             `There was a problem there was a problem updating {{tablet_alias}}'s metatdata:`);
    this.dialogSettings.setMessage('Changed metadata in the topology server for {{tablet_alias}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new TabExtRepFlags(this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'TabletExternallyReparented');
    this.dialogSettings.toggleModal();
  }

  openPlanRepShardDialog() {
    this.dialogSettings = new DialogSettings('Reparent', `Plan to reparent a shard`, '',
                                             'There was a problem initializing a shard master for {{shard_ref}}:');
    this.dialogSettings.setMessage('Reparented {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PlanRepShardFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'PlannedReparentShard');
    this.dialogSettings.toggleModal();
  }

  openEmergencyRepShardDialog() {
    this.dialogSettings = new DialogSettings('Reparent', `Emergency Reparent Shard`, '',
                                             'There was a problem initializing a shard master for {{shard_ref}}:');
    this.dialogSettings.setMessage('Initialized shard master for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new EmergencyRepShardFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'EmergencyReparentShard');
    this.dialogSettings.toggleModal();
  }

  openShardReplicationPosDialog() {
    this.dialogSettings = new DialogSettings('Get', `Get ${this.shardName} Replication Positions`, '',
                                             'There was a problem initializing a shard master for {{shard_ref}}:');
    this.dialogSettings.setMessage('Fetched Replication Positions for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new ShardReplicationPosFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ShardReplicationPositions');
    this.dialogSettings.toggleModal();
  }

  openValidateVerShardDialog() {
    this.dialogSettings = new DialogSettings('Validate', `Validate ${this.shardName}'s Version`, '',
                                             'There was a problem initializing a shard master for {{shard_ref}}:');
    this.dialogSettings.setMessage(`Validated {{shard_ref}}'s Version`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new ValidateVerShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ValidateVersionShard');
    this.dialogSettings.toggleModal();
  }

  openDeleteTabletDialog() {
    this.dialogSettings = new DialogSettings('Delete', `Delete ${this.selectedTablet.label}`,
                                             `Are you sure you want to delete ${this.selectedTablet.label}?`,
                                             `There was a problem deleting ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Deleted ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new DeleteTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'DeleteTablet');
    this.dialogSettings.toggleModal();
  }

  openRefreshTabletDialog() {
    this.dialogSettings = new DialogSettings('Refresh', `Refresh ${this.selectedTablet.label}`, '',
                                             `There was a problem refreshing ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Refreshed ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new RefreshTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'RefreshState');
    this.dialogSettings.toggleModal();
  }

  openPingTabletDialog() {
    this.dialogSettings = new DialogSettings('Ping', `Ping ${this.selectedTablet.label}`, '',
                                             `There was a problem pinging ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Pinged ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'Ping');
    this.dialogSettings.toggleModal();
  }

  openSetReadOnlyDialog() {
    this.dialogSettings = new DialogSettings('Set', `Set ${this.selectedTablet.label} to Read Only`, '',
                                             `There was a problem setting ${this.selectedTablet.label} to Read Only:`);
    this.dialogSettings.setMessage(`Set ${this.selectedTablet.label} to Read Only`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'SetReadOnly');
    this.dialogSettings.toggleModal();
  }

  openSetReadWriteDialog() {
    this.dialogSettings = new DialogSettings('Set', `Set ${this.selectedTablet.label} to Read/Write`, '',
                                             `There was a problem setting ${this.selectedTablet.label} to Read/Write:`);
    this.dialogSettings.setMessage(`Set ${this.selectedTablet.label} to Read/Write`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'SetReadWrite');
    this.dialogSettings.toggleModal();
  }

  openStartSlaveDialog() {
    this.dialogSettings = new DialogSettings('Start', `Start Slave, ${this.selectedTablet.label}`, '',
                                             `There was a problem starting slave, ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Started Slave, ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'StartSlave');
    this.dialogSettings.toggleModal();
  }

  openStopSlaveDialog() {
    this.dialogSettings = new DialogSettings('Stop', `Stop Slave, ${this.selectedTablet.label}`, '',
                                             `There was a problem stopping slave, ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Stopped Slave, ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'StopSlave');
    this.dialogSettings.toggleModal();
  }

  openRunHealthCheckDialog() {
    this.dialogSettings = new DialogSettings('Run', `Run Health Check on ${this.selectedTablet.label}`, '',
                                             `There was a problem running Health Check on ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Ran Health Check on ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'RunHealthCheck');
    this.dialogSettings.toggleModal();
  }

  openIgnoreHealthErrorDialog() {
    this.dialogSettings = new DialogSettings('Ignore', `Ignore Health Check for ${this.selectedTablet.label}`, '',
                                             `There was a problem ignoring the Health Check for ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Ignored ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'IgnoreHealthError');
    this.dialogSettings.toggleModal();
  }

  openDemoteMasterDialog() {
    this.dialogSettings = new DialogSettings('Demote', `Demote ${this.selectedTablet.label}`, '',
                                             `There was a problem demoting ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Demoted ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'DemoteMaster');
    this.dialogSettings.toggleModal();
  }

  openReparentTabletDialog() {
    this.dialogSettings = new DialogSettings('Reparent', `Reparent ${this.selectedTablet.label}`, '',
                                             `There was a problem reparenting ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Reparented ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'ReparentTablet');
    this.dialogSettings.toggleModal();
  }

  refreshShardView() {
    this.getKeyspace(this.keyspaceName);
    this.tabletActions = this.getTabletActions();
  }

  navigateToKeyspace(dialogContent: DialogContent) {
    this.router.navigate(['/keyspace'], {queryParams: {keyspace: this.keyspaceName}});
  }

  navigate(tablet: any) {
    this.router.navigate(['/tablet'], {queryParams: {keyspace: this.keyspaceName, shard: this.shardName, tablet: tablet.alias}});
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
