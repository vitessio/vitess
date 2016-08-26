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

  deleteTablet() {
    this.runCommand('DeleteTablet', 'There was a problem deleting {{tablet_alias}}:');
  }

  refreshTablet() {
    this.runCommand('RefreshState', 'There was a problem refreshing {{tablet_alias}}:');
  }

  pingTablet() {
    this.runCommand('Ping', 'There was a problem pinging {{tablet_alias}}:');
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

  openDeleteTabletDialog() {
    this.dialogSettings = new DialogSettings('Delete', this.deleteTablet.bind(this),
                                             `Delete ${this.tablet.label}`, `Are you sure you want to delete ${this.tablet.label}?`);
    this.dialogSettings.setMessage('Deleted {{tablet_alias}}');
    this.dialogSettings.onCloseFunction = this.navigateToShard.bind(this);
    let flags = new DeleteTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags);
    this.dialogSettings.toggleModal();
  }

  openRefreshTabletDialog() {
    this.dialogSettings = new DialogSettings('Refresh', this.refreshTablet.bind(this), `Refresh ${this.tablet.label}`);
    this.dialogSettings.setMessage('Refreshed {{tablet_alias}}');
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new RefreshTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags);
    this.dialogSettings.toggleModal();
  }

  openPingTabletDialog() {
    this.dialogSettings = new DialogSettings('Ping', this.pingTablet.bind(this), `Ping ${this.tablet.label}`);
    this.dialogSettings.setMessage('Pinged {{tablet_alias}}');
    this.dialogSettings.onCloseFunction = this.refreshTabletView.bind(this);
    let flags = new PingTabletFlags(this.tablet.ref).flags;
    this.dialogContent = new DialogContent('tablet_alias', flags);
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

  navigate(tablet) {
    this.router.navigate(['/tablet'], {queryParams: {keyspace: this.keyspaceName, shard: this.shardName, tablet: tablet.ref}});
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
