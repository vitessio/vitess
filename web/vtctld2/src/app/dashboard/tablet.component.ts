import { ActivatedRoute, Router } from '@angular/router';
import { Component, OnInit, OnDestroy } from '@angular/core';
import { DomSanitizationService, SafeResourceUrl } from '@angular/platform-browser';

import { Observable } from 'rxjs/Observable';

import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { DeleteTabletFlags, PingTabletFlags, RefreshTabletFlags } from '../shared/flags/tablet.flags';
import { TabletService } from '../api/tablet.service';
import { VtctlService } from '../api/vtctl.service';

@Component({
  selector: 'vt-tablet-view',
  templateUrl: './tablet.component.html',
  styleUrls: [
    './tablet.component.css',
    '../styles/vt.style.css'
  ],
})
export class TabletComponent implements OnInit, OnDestroy {
  private routeSub: any;
  keyspaceName: string;
  shardName: string;
  tabletRef: string;
  tablet: any;
  tabletUrl: SafeResourceUrl;
  tabletReady = false;
  dialogSettings: DialogSettings;
  dialogContent: DialogContent;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private tabletService: TabletService,
    private vtctlService: VtctlService,
    private sanitizer: DomSanitizationService) {}

  ngOnInit() {
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();

    this.routeSub = this.route.queryParams.subscribe(params => {
      let keyspaceName = params['keyspace'];
      let shardName = params['shard'];
      let tabletRef = params['tablet'];
      if (keyspaceName && shardName && tabletRef) {
        this.keyspaceName = keyspaceName;
        this.shardName = shardName;
        this.tabletRef = tabletRef;
        this.getTablet(tabletRef);
      }
    });
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
  }

  getTablet(tabletRef: string) {
    this.tabletService.getTablet(tabletRef).subscribe((tablet) => {
      this.tablet = tablet;
      this.tabletUrl = this.sanitizer.bypassSecurityTrustResourceUrl(`http://${tablet.hostname}:${tablet.port_map.vt}`);
      this.tabletReady = true;
    });
  }

  openDeleteTabletDialog() {
    this.dialogSettings = new DialogSettings('Delete', `Delete ${this.tablet.label}`,
                                             `Are you sure you want to delete ${this.tablet.label}?`,
                                             `There was a problem deleting ${this.tablet.label}:`);
    this.dialogSettings.setMessage(`Deleted ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.navigateToShard.bind(this);
    let flags = new DeleteTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'DeleteTablet');
    this.dialogSettings.toggleModal();
  }

  openRefreshTabletDialog() {
    this.dialogSettings = new DialogSettings('Refresh', `Refresh ${this.tablet.label}`, '',
                                             `There was a problem refreshing ${this.tablet.label}:`);
    this.dialogSettings.setMessage(`Refreshed ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new RefreshTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'RefreshState');
    this.dialogSettings.toggleModal();
  }

  openPingTabletDialog() {
    this.dialogSettings = new DialogSettings('Ping', `Ping ${this.tablet.label}`, '', `There was a problem pinging ${this.tablet.label}:`);
    this.dialogSettings.setMessage(`Pinged ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'Ping');
    this.dialogSettings.toggleModal();
  }

  openSetReadOnlyDialog() {
    this.dialogSettings = new DialogSettings('Set', `Set ${this.tablet.label} to Read Only`, '',
                                             `There was a problem setting ${this.tablet.label} to Read Only:`);
    this.dialogSettings.setMessage(`Set ${this.tablet.label} to Read Only`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'SetReadOnly');
    this.dialogSettings.toggleModal();
  }

  openSetReadWriteDialog() {
    this.dialogSettings = new DialogSettings('Set', `Set ${this.tablet.label} to Read/Write`, '',
                                             `There was a problem setting ${this.tablet.label} to Read/Write:`);
    this.dialogSettings.setMessage(`Set ${this.tablet.label} to Read/Write`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'SetReadWrite');
    this.dialogSettings.toggleModal();
  }

  openStartSlaveDialog() {
    this.dialogSettings = new DialogSettings('Start', `Start Replication, ${this.tablet.label}`, '',
                                             `There was a problem starting replication on, ${this.tablet.label}:`);
    this.dialogSettings.setMessage(`Started Replication, ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'StartSlave');
    this.dialogSettings.toggleModal();
  }

  openStopSlaveDialog() {
    this.dialogSettings = new DialogSettings('Stop', `Stop Replication, ${this.tablet.label}`, '',
                                             `There was a problem stopping replication on, ${this.tablet.label}:`);
    this.dialogSettings.setMessage(`Stopped Replication, ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'StopSlave');
    this.dialogSettings.toggleModal();
  }

  openRunHealthCheckDialog() {
    this.dialogSettings = new DialogSettings('Run', `Run Health Check on ${this.tablet.label}`, '',
                                             `There was a problem running Health Check on ${this.tablet.label}:`);
    this.dialogSettings.setMessage(`Ran Health Check on ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'RunHealthCheck');
    this.dialogSettings.toggleModal();
  }

  openIgnoreHealthErrorDialog() {
    this.dialogSettings = new DialogSettings('Ignore', `Ignore Health Check for ${this.tablet.label}`, '',
                                             `There was a problem ignoring the Health Check for ${this.tablet.label}:`);
    this.dialogSettings.setMessage(`Ignored ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'IgnoreHealthError');
    this.dialogSettings.toggleModal();
  }

  openReparentTabletDialog() {
    this.dialogSettings = new DialogSettings('Reparent', `Reparent ${this.tablet.label}`, '',
                                             `There was a problem reparenting ${this.tablet.label}:`);
    this.dialogSettings.setMessage(`Reparented ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'ReparentTablet');
    this.dialogSettings.toggleModal();
  }

  refreshTabletView() {
    this.getTablet(this.tabletRef);
    // Force tablet url to refresh
    this.tabletUrl = this.tabletUrl;
  }

  navigateToShard(dialogContent: DialogContent) {
    this.router.navigate(['/shard'], {queryParams: {keyspace: this.keyspaceName, shard: this.shardName}});
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
