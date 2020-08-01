import { ActivatedRoute, Router } from '@angular/router';
import { Component, OnDestroy, OnInit } from '@angular/core';

import { Observable } from 'rxjs/Observable';

import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { DeleteShardFlags, InitShardMasterFlags, ValidateShardFlags,
         TabExtRepFlags, PlanRepShardFlags, EmergencyRepShardFlags,
         ShardReplicationPosFlags, ReloadSchemaShardFlags,
         ValidateVerShardFlags } from '../shared/flags/shard.flags';
import { DeleteTabletFlags, IgnoreHealthCheckFlags, PingTabletFlags, RefreshTabletFlags } from '../shared/flags/tablet.flags';
import { FeaturesService } from '../api/features.service';
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
  inFlightQueries = 0;
  noTablets = false;
  // selectedTablet is populated when we click on an Actions menu.
  selectedTablet = undefined;
  dialogSettings: DialogSettings;
  dialogContent: DialogContent;
  private actions: MenuItem[];
  private tabletActionsMaster: MenuItem[];
  private tabletActionsSlave: MenuItem[];

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private featuresService: FeaturesService,
    private keyspaceService: KeyspaceService,
    private tabletService: TabletService,
    private vtctlService: VtctlService) {
  }


  ngOnInit() {
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();
    this.refreshMenus();
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

  refreshMenus() {
    this.actions = this.getActions();
    this.tabletActionsMaster = this.getTabletActions(true);
    this.tabletActionsSlave = this.getTabletActions(false);
  }

  // getActions returns the actions on a shard.
  getActions() {
    let result: MenuItem[] = [
      {label: 'Status', items: [
        {label: 'Validate Shard', command: (event) => {this.openValidateShardDialog(); }},
        {label: 'Validate Versions', command: (event) => {this.openValidateVerShardDialog(); }},
        {label: 'Shard Replication Positions', command: (event) => {this.openShardReplicationPosDialog(); }},
      ]},
      {label: 'Reload', items: [
        {label: 'Reload Schema in Shard', command: (event) => {this.openReloadSchemaShardDialog(); }},
      ]},
      {label: 'Change', items: [
        {label: 'Externally Reparent', command: (event) => {this.openTabExtRepDialog(); }},
      ]},
    ];
    if (this.featuresService.showTopologyCRUD) {
      // Push a new individual item into the 'Change' section.
      result[2].items.push({label: 'Delete Shard', command: (event) => {this.openDeleteShardDialog(); }});
    }
    if (this.featuresService.activeReparents) {
      // Push an entire new section.
      result.push(
        {label: 'Reparent', items: [
          {label: 'Initialize Shard Master', command: (event) => {this.openInitShardMasterDialog(); }},
          {label: 'Planned Reparent', command: (event) => {this.openPlanRepShardDialog(); }},
          {label: 'Emergency Reparent', command: (event) => {this.openEmergencyRepShardDialog(); }},
        ]}
      );
    }
    return result;
  }

  // getTabletActions returns the actions on a tablet.
  getTabletActions(isMaster: boolean) {
    let result: MenuItem[] = [
      {label: 'Status', items: [
        {label: 'Ping', command: (event) => {this.openPingTabletDialog(); }},
        {label: 'Refresh State', command: (event) => {this.openRefreshTabletDialog(); }},
        {label: 'Run Health Check', command: (event) => {this.openRunHealthCheckDialog(); }},
      ]},
      {label: 'Change', items: [
        {label: 'Ignore Health Error', command: (event) => {this.openIgnoreHealthErrorDialog(); }},
        {label: 'Delete Tablet', command: (event) => {this.openDeleteTabletDialog(); }},
      ]},
    ];
    if (!isMaster) {
      // Add replication-related methods to the 'Change' section for slaves.
      result[1].items.push(
        {label: 'Start Replication', command: (event) => {this.openStartSlaveDialog(); }},
        {label: 'Stop Replication', command: (event) => {this.openStopSlaveDialog(); }},
      );
    }
    if (this.featuresService.activeReparents) {
      // Add a couple new items to the 'Change' section for master.
      if (isMaster) {
        result[1].items.push(
          {label: 'Set ReadOnly', command: (event) => {this.openSetReadOnlyDialog(); }},
          {label: 'Set ReadWrite', command: (event) => {this.openSetReadWriteDialog(); }},
        );
      }
      // Add an entire section for Reparent for slaves.
      if (!isMaster) {
        result.push(
          {label: 'Reparent', items: [
            {label: 'Reparent Tablet', command: (event) => {this.openReparentTabletDialog(); }},
          ]}
        );
      }
    }
    return result;
  }

  portNames(tablet) {
    return Object.keys(tablet.port_map);
  }

  getUrl(tablet) {
    return `http://${tablet.hostname}:${tablet.port_map.vt}`;
  }

  // typeIndex returns a sortable string for a tablet type.
  // We make master first, then replica, then rdonly, then
  // any other type alphabetically ordered.
  typeIndex(t) {
    if (t === 'master') {
      return '0master';
    }
    if (t === 'replica') {
      return '1replica';
    }
    if (t === 'rdonly') {
      return '2rdonly';
    }
    return t;
  }

  // sortByType sorts the tablets by the following sort orders:
  // 1. Type, using typeIndex.
  // 2. Cell
  // 3. UID.
  sortByType(event) {
    let t = this;
    this.tablets.sort(function(t1, t2) {
      // Primary sort order: type.
      let s1 = t.typeIndex(t1.type);
      let s2 = t.typeIndex(t2.type);
      if (s1 < s2) {
        return -1 * event.order;
      }
      if (s1 > s2) {
        return 1 * event.order;
      }

      // Secondary sort order: cell.
      if (t1.cell < t2.cell) {
        return -1 * event.order;
      }
      if (t1.cell > t2.cell) {
        return 1 * event.order;
      }

      // Tertiary sort order: UID.
      if (t1.uid < t2.uid) {
        return -1 * event.order;
      }
      if (t1.uid > t2.uid) {
        return 1 * event.order;
      }

      return 0;
    });
  }

  // sortByCell sorts the tablets by the following sort orders:
  // 1. Cell
  // 2. Type, using typeIndex. Not using event.order.
  // 3. UID. Not using event.order.
  //
  // Note we keep the order within a cell the same, whether it's ascending
  // or descending sort.
  // So the final order will be something like this, for ascending:
  //   cell1  master
  //   cell1  replica1.1
  //   cell1  replica1.2
  //   cell2  replica2.1
  //   cell2  replica2.2
  // or this for descending:
  //   cell2  replica2.1
  //   cell2  replica2.2
  //   cell1  master
  //   cell1  replica1.1
  //   cell1  replica1.2
  sortByCell(event) {
    let t = this;
    this.tablets.sort(function(t1, t2) {
      // Primary sort order: cell.
      if (t1.cell < t2.cell) {
        return -1 * event.order;
      }
      if (t1.cell > t2.cell) {
        return 1 * event.order;
      }

      // Secondary sort order: type. Not using event.order.
      let s1 = t.typeIndex(t1.type);
      let s2 = t.typeIndex(t2.type);
      if (s1 < s2) {
        return -1;
      }
      if (s1 > s2) {
        return 1;
      }

      // Tertiary sort order: UID. Not using event.order.
      if (t1.uid < t2.uid) {
        return -1;
      }
      if (t1.uid > t2.uid) {
        return 1;
      }

      return 0;
    });
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
  }

  // getKeyspace is called on init or refresh to fetch the keyspace.
  // It triggers getTabletList if the shard exists in the keyspace.
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
        // Refresh the menus, at this point we read the features map.
        this.refreshMenus();

        this.keyspace = keyspace;
        // Test to see if shard is in keyspace
        for (let shard of this.keyspace['servingShards'].concat(this.keyspace['nonservingShards'])){
          if (shard === this.shardName) {
            this.getTabletList(this.keyspaceName, this.shardName);
            return;
          }
        }
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

  // getTabletList is called after getting the keyspace.
  // It will trigger getTablet for each tablet in the shard.
  getTabletList(keyspaceName: string, shardName: string) {
    this.tablets = [];
    this.noTablets = false;
    let shardRef = `${keyspaceName}/${shardName}`;
    this.inFlightQueries++;
    this.tabletService.getTabletList(shardRef).subscribe((tabletList) => {
      if (tabletList.length === 0) {
        this.noTablets = true;
        return;
      }
      for (let tablet of tabletList){
        this.getTablet(`${tablet.cell}-${tablet.uid}`);
      }
    }, () => {
      console.log('Error getting tablet list');
      this.inFlightQueries--;
    }, () => {
      this.inFlightQueries--;
    });
  }

  // getTablet is triggered for each tablet in the shard.
  // After adding each tablet, we sort the list, so the initial display
  // has master first, then replica, then rdonly, then the rest.
  getTablet(tabletRef: string) {
    this.inFlightQueries++;
    this.tabletService.getTablet(tabletRef).subscribe((tablet) => {
      this.tablets.push(tablet);
    }, () => {
      console.log('Error getting tablet');
      this.inFlightQueries--;
    }, () => {
      this.inFlightQueries--;
      if (this.inFlightQueries === 0) {
        // We just got the last tablet, we can now sort them.
        this.sortByType({'order': 1});
      }
    });
  }

  openDeleteShardDialog() {
    this.dialogSettings = new DialogSettings('Delete Shard', `Delete Shard ${this.keyspaceName}/${this.shardName}`,
                                             `Are you sure you want to delete ${this.keyspaceName}/${this.shardName}?`,
                                             'There was a problem deleting shard {{shard_ref}}:');
    this.dialogSettings.setMessage('Deleted {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.navigateToKeyspace.bind(this);
    let flags = new DeleteShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'DeleteShard');
    this.dialogSettings.toggleModal();
  }

  openValidateShardDialog() {
    this.dialogSettings = new DialogSettings('Validate Shard', `Validate Shard ${this.keyspaceName}/${this.shardName}`, '',
                                             'There was a problem validating shard {{shard_ref}}:');
    this.dialogSettings.setMessage('Validated {{shard_ref}}');
    let flags = new ValidateShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ValidateShard');
    this.dialogSettings.toggleModal();
  }

  openInitShardMasterDialog() {
    this.dialogSettings = new DialogSettings('Initialize Shard Master', `Initialize Shard ${this.keyspaceName}/${this.shardName} Master`,
                                             'This will reset the replication on all hosts and make the designated tablet the master.',
                                             'There was a problem initializing a shard master for {{shard_ref}}:');
    this.dialogSettings.setMessage('Initialized shard master for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new InitShardMasterFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'InitShardMaster');
    this.dialogSettings.toggleModal();
  }

  openTabExtRepDialog() {
    this.dialogSettings = new DialogSettings('TabletExternallyReparented',
                                             `Run Tablet Externally Reparented in Shard ${this.keyspaceName}/${this.shardName}`,
                                             'The chosen tablet will be considered the shard master' +
                                             ' (but Vitess won\'t change the replication setup).',
                                             `There was a problem running TabletExternallyReparented:`);
    this.dialogSettings.setMessage('Ran TabletExternallyReparented');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new TabExtRepFlags(this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'TabletExternallyReparented');
    this.dialogSettings.toggleModal();
  }

  openPlanRepShardDialog() {
    this.dialogSettings = new DialogSettings('Reparent', `Planned Reparent in Shard ${this.keyspaceName}/${this.shardName}`,
                                             'Failover to a new master. Assumes the old master is still healthy and reachable.',
                                             'There was a problem reparenting shard {{shard_ref}}:');
    this.dialogSettings.setMessage('Reparented {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PlanRepShardFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'PlannedReparentShard');
    this.dialogSettings.toggleModal();
  }

  openEmergencyRepShardDialog() {
    this.dialogSettings = new DialogSettings('Reparent', `Emergency Reparent in Shard ${this.keyspaceName}/${this.shardName}`,
                                             'Failover to a new master. Assumes the old master is not reachable.',
                                             'There was a problem with emergency reparenting shard {{shard_ref}}:');
    this.dialogSettings.setMessage('Initialized shard master for {{shard_ref}}');
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new EmergencyRepShardFlags(this.keyspaceName, this.shardName, this.tablets).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {tablet_alias: true}, undefined, 'EmergencyReparentShard');
    this.dialogSettings.toggleModal();
  }

  openValidateVerShardDialog() {
    this.dialogSettings = new DialogSettings('Validate Shard', `Validate Shard ${this.keyspaceName}/${this.shardName} Versions`,
                                             `Creates a report of software versions for ${this.keyspaceName}/${this.shardName}.`,
                                             'There was a problem validating shard {{shard_ref}} versions:');
    this.dialogSettings.setMessage(`Validated {{shard_ref}}'s versions`);
    let flags = new ValidateVerShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ValidateVersionShard');
    this.dialogSettings.toggleModal();
  }

  openShardReplicationPosDialog() {
    this.dialogSettings = new DialogSettings('Get', `Get ${this.keyspaceName}/${this.shardName} Replication Positions`,
                                             'Displays the replication position for all tablets in a shard.',
                                             'There was a problem getting replication positions for shard {{shard_ref}}:');
    this.dialogSettings.setMessage('Fetched Replication Positions for {{shard_ref}}');
    let flags = new ShardReplicationPosFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ShardReplicationPositions');
    this.dialogSettings.toggleModal();
  }

  openReloadSchemaShardDialog() {
    this.dialogSettings = new DialogSettings('Reload Schema', `Reload Schema in ${this.keyspaceName}/${this.shardName}`, '',
                                             'There was a problem reloading schema in shard {{shard_ref}}:');
    this.dialogSettings.setMessage('Reloaded {{shard_ref}} Schema');
    let flags = new ReloadSchemaShardFlags(this.keyspaceName, this.shardName).flags;
    this.dialogContent = new DialogContent(this.shardName, flags, {}, undefined, 'ReloadSchemaShard');
    this.dialogSettings.toggleModal();
  }

  openDeleteTabletDialog() {
    this.dialogSettings = new DialogSettings('Delete Tablet', `Delete ${this.selectedTablet.label}`,
                                             `Are you sure you want to delete ${this.selectedTablet.label}?` +
                                             ` It will be removed from the topology (but vttablet and MySQL won't be touched).`,
                                             `There was a problem deleting ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Deleted ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new DeleteTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'DeleteTablet');
    this.dialogSettings.toggleModal();
  }

  openRefreshTabletDialog() {
    this.dialogSettings = new DialogSettings('Refresh', `Refresh ${this.selectedTablet.label}`,
                                             `Makes ${this.selectedTablet.label} re-read its topology record` +
                                             ` and adjust its internal state to match.`,
                                             `There was a problem refreshing ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Refreshed ${this.selectedTablet.label}`);
    let flags = new RefreshTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'RefreshState');
    this.dialogSettings.toggleModal();
  }

  openPingTabletDialog() {
    this.dialogSettings = new DialogSettings('Ping', `Ping ${this.selectedTablet.label}`,
                                             `See if tablet ${this.selectedTablet.label} is reachable via RPC.`,
                                             `There was a problem pinging ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Pinged ${this.selectedTablet.label}`);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'Ping');
    this.dialogSettings.toggleModal();
  }

  openSetReadOnlyDialog() {
    this.dialogSettings = new DialogSettings('Set Read-Only', `Set ${this.selectedTablet.label} to Read-Only`,
                                             'Can be used on a master to disable writing. Use with caution.',
                                             `There was a problem setting ${this.selectedTablet.label} to Read-Only:`);
    this.dialogSettings.setMessage(`Set ${this.selectedTablet.label} to Read-Only`);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'SetReadOnly');
    this.dialogSettings.toggleModal();
  }

  openSetReadWriteDialog() {
    this.dialogSettings = new DialogSettings('Set Read-Write', `Set ${this.selectedTablet.label} to Read-Write`,
                                             'Can be used on a master to re-enable writing. Use with caution.',
                                             `There was a problem setting ${this.selectedTablet.label} to Read-Write:`);
    this.dialogSettings.setMessage(`Set ${this.selectedTablet.label} to Read-Write`);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'SetReadWrite');
    this.dialogSettings.toggleModal();
  }

  openStartSlaveDialog() {
    this.dialogSettings = new DialogSettings('Start', `Start replication on ${this.selectedTablet.label}`,
                                             `Restart replication on ${this.selectedTablet.label}.`,
                                             `There was a problem starting replication on ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Started replication on ${this.selectedTablet.label}`);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'StartSlave');
    this.dialogSettings.toggleModal();
  }

  openStopSlaveDialog() {
    this.dialogSettings = new DialogSettings('Stop', `Stop replication on ${this.selectedTablet.label}`,
                                             `Stop replication on ${this.selectedTablet.label}. May render the tablet unhealthy.`,
                                             `There was a problem stopping replication on ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Stopped replication on ${this.selectedTablet.label}`);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'StopSlave');
    this.dialogSettings.toggleModal();
  }

  openRunHealthCheckDialog() {
    this.dialogSettings = new DialogSettings('Health Check', `Run Health Check on ${this.selectedTablet.label}`,
                                             `Asks tablet ${this.selectedTablet.label} to run its health check.`,
                                             `There was a problem running Health Check on ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Ran Health Check on ${this.selectedTablet.label}`);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'RunHealthCheck');
    this.dialogSettings.toggleModal();
  }

  openIgnoreHealthErrorDialog() {
    this.dialogSettings = new DialogSettings('Ignore', `Ignore Health Check for ${this.selectedTablet.label}`,
                                             `Make ${this.selectedTablet.label} ignore certain health errors that match a pattern.` +
                                             ` Vitess may then use that tablet even if it appears unhealthy.`,
                                             `There was a problem ignoring the Health Check for ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Ignored ${this.selectedTablet.label}`);
    let flags = new IgnoreHealthCheckFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'IgnoreHealthError');
    this.dialogSettings.toggleModal();
  }

  openReparentTabletDialog() {
    this.dialogSettings = new DialogSettings('Reparent', `Reparent ${this.selectedTablet.label}`,
                                             `Re-connect the replication for ${this.selectedTablet.label} to the current master.` +
                                             ` Use if ${this.selectedTablet.label} was not reachable during the previous reparent.`,
                                             `There was a problem reparenting ${this.selectedTablet.label}:`);
    this.dialogSettings.setMessage(`Reparented ${this.selectedTablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshShardView.bind(this);
    let flags = new PingTabletFlags(this.selectedTablet.alias).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'ReparentTablet');
    this.dialogSettings.toggleModal();
  }

  refreshShardView() {
    this.getKeyspace(this.keyspaceName);
    this.refreshMenus();
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
