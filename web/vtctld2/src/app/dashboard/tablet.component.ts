import { ActivatedRoute, Router, ROUTER_DIRECTIVES } from '@angular/router';

import { Component, OnInit, OnDestroy } from '@angular/core';

import {DomSanitizationService, SafeResourceUrl} from '@angular/platform-browser';

import { Observable } from 'rxjs/Observable';

import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';

import { Accordion, AccordionTab } from 'primeng/primeng';

import { DialogComponent } from '../shared/dialog/dialog.component';
import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { DeleteTabletFlags, PingTabletFlags, RefreshTabletFlags } from '../shared/flags/tablet.flags';
import { ShardService } from '../api/shard.service';
import { TabletService } from '../api/tablet.service';
import { VtctlService } from '../api/vtctl.service';

@Component({
  selector: 'vt-tablet-view',
  templateUrl: './tablet.component.html',
  styleUrls: [
    './tablet.component.css',
    '../styles/vt.style.css'
  ],
  directives: [
    ROUTER_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    MD_BUTTON_DIRECTIVES,
    DialogComponent,
    Accordion,
    AccordionTab
  ],
  providers: [
    ShardService,
    TabletService,
    VtctlService,
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
  refresh = true;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private tabletService: TabletService,
    private vtctlService: VtctlService,
    private sanitizer: DomSanitizationService) {}

  ngOnInit() {
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();
    let paramStream = this.router.routerState.queryParams;
    let routeStream = this.route.url;
    this.routeSub = paramStream.combineLatest(routeStream).subscribe( routeData => {
      let params = routeData[0];
      let path = routeData[1][0].path;
      let keyspaceName = params['keyspace'];
      let shardName = params['shard'];
      let tabletRef = params['tablet'];
      if (path === 'tablet' && keyspaceName && shardName && tabletRef) {
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

  deleteTablet() {
    this.serverCall(`There was a problem deleting ${this.tablet.label}:`);
  }

  refreshTablet() {
    this.serverCall(`There was a problem refreshing ${this.tablet.label}:`);
  }

  pingTablet() {
    this.serverCall(`There was a problem pinging ${this.tablet.label}:`);
  }

  SetReadOnly() {
    this.serverCall(`There was a problem setting ${this.tablet.label} to Read Only:`);
  }

  SetReadWrite() {
    this.serverCall(`There was a problem setting ${this.tablet.label} to Read/Write:`);
  }

  StartSlave() {
    this.serverCall(`There was a problem starting slave, ${this.tablet.label}:`);
  }

  StopSlave() {
    this.serverCall(`There was a problem stopping slave, ${this.tablet.label}:`);
  }

  RunHealthCheck() {
    this.serverCall(`There was a problem running Health Check on ${this.tablet.label}:`);
  }

  IgnoreHealthError() {
    this.serverCall(`There was a problem ignoring the Health Check for ${this.tablet.label}:`);
  }

  DemoteMaster() {
    this.serverCall(`There was a problem demoting ${this.tablet.label}:`);
  }

  ReparentTablet() {
    this.serverCall(`There was a problem reparenting ${this.tablet.label}:`);
  }

  serverCall(errorMessage: string) {
    this.vtctlService.serverCall('', this.dialogContent, this.dialogSettings, errorMessage);
  }

  prepareDeleteTablet() {
    this.dialogSettings = new DialogSettings('Delete', this.deleteTablet.bind(this),
                                             `Delete ${this.tablet.label}`, `Are you sure you want to delete ${this.tablet.label}?`);
    this.dialogSettings.setMessage(`Deleted ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.navigateToShard.bind(this);
    let flags = new DeleteTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'DeleteTablet');
    this.dialogSettings.toggleModal();
  }

  prepareRefreshTablet() {
    this.dialogSettings = new DialogSettings('Refresh', this.refreshTablet.bind(this), `Refresh ${this.tablet.label}`);
    this.dialogSettings.setMessage(`Refreshed ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new RefreshTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'RefreshState');
    this.dialogSettings.toggleModal();
  }

  preparePingTablet() {
    this.dialogSettings = new DialogSettings('Ping', this.pingTablet.bind(this), `Ping ${this.tablet.label}`);
    this.dialogSettings.setMessage(`Pinged ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'Ping');
    this.dialogSettings.toggleModal();
  }

  prepareSetReadOnly() {
    this.dialogSettings = new DialogSettings('Set', this.SetReadOnly.bind(this), `Set ${this.tablet.label} to Read Only`);
    this.dialogSettings.setMessage(`Set ${this.tablet.label} to Read Only`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'SetReadOnly');
    this.dialogSettings.toggleModal();
  }

  prepareSetReadWrite() {
    this.dialogSettings = new DialogSettings('Set', this.SetReadWrite.bind(this), `Set ${this.tablet.label} to Read/Write`);
    this.dialogSettings.setMessage(`Set ${this.tablet.label} to Read/Write`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'SetReadWrite');
    this.dialogSettings.toggleModal();
  }

  prepareStartSlave() {
    this.dialogSettings = new DialogSettings('Start', this.StartSlave.bind(this), `Start Slave, ${this.tablet.label}`);
    this.dialogSettings.setMessage(`Started Slave, ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'StartSlave');
    this.dialogSettings.toggleModal();
  }

  prepareStopSlave() {
    this.dialogSettings = new DialogSettings('Stop', this.StopSlave.bind(this), `Stop Slave, ${this.tablet.label}`);
    this.dialogSettings.setMessage(`Stopped Slave, ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'StopSlave');
    this.dialogSettings.toggleModal();
  }

  prepareRunHealthCheck() {
    this.dialogSettings = new DialogSettings('Run', this.RunHealthCheck.bind(this), `Run Health Check on ${this.tablet.label}`);
    this.dialogSettings.setMessage(`Ran Health Check on ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'RunHealthCheck');
    this.dialogSettings.toggleModal();
  }

  prepareIgnoreHealthError() {
    this.dialogSettings = new DialogSettings('Ignore', this.IgnoreHealthError.bind(this), `Ignore Health Check for ${this.tablet.label}`);
    this.dialogSettings.setMessage(`Ignored ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'IgnoreHealthError');
    this.dialogSettings.toggleModal();
  }

  prepareDemoteMaster() {
    this.dialogSettings = new DialogSettings('Demote', this.DemoteMaster.bind(this), `Demote ${this.tablet.label}`);
    this.dialogSettings.setMessage(`Demoted ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'DemoteMaster');
    this.dialogSettings.toggleModal();
  }

  prepareReparentTablet() {
    this.dialogSettings = new DialogSettings('Reparent', this.ReparentTablet.bind(this), `Reparent ${this.tablet.label}`);
    this.dialogSettings.setMessage(`Reparented ${this.tablet.label}`);
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags, {}, undefined, 'ReparentTablet');
    this.dialogSettings.toggleModal();
  }


  refreshTabletView() {
    this.getTablet(this.tabletRef);
    this.tabletUrl = this.tabletUrl;
  }

  navigateToShard(dialogContent: DialogContent) {
    this.router.navigate(['/shard'], {queryParams: {keyspace: this.keyspaceName, shard: this.shardName}});
  }

  navigate(tablet) {
    this.router.navigate(['/tablet'], {queryParams: {keyspace: this.keyspaceName, shard: this.shardName, tablet: tablet.ref}});
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
