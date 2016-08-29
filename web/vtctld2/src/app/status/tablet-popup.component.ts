import { Component, Input, OnInit, NgZone } from '@angular/core';
import { DomSanitizationService, SafeResourceUrl } from '@angular/platform-browser';

import { TabletStatusService } from '../api/tablet-status.service';

@Component({
  selector: 'vt-tablet-popup',
    templateUrl: './tablet-popup.component.html',
    styleUrls: ['./tablet-popup.component.css'],
})

export class TabletPopupComponent implements OnInit {
  @Input() data: number;
  @Input() alias: any;
  @Input() keyspace: string;
  @Input() shard: string;
  @Input() cell: string;
  @Input() tabletType: string;
  @Input() clicked: boolean;

  title: string;
  hostname: string;
  tabletUrl: SafeResourceUrl;
  lag: number;
  qps: number;
  serving: boolean;
  error: string;
  lastError: string;
  dataToDisplay: Array<any> = [];

  aggregated = false;
  unaggregated = false;

  defaultPopup = false;
  hoverPopup = false;
  clickPopup = false;

  healthDataReady = false;

  constructor(private tabletService: TabletStatusService, private zone: NgZone,
              private sanitizer: DomSanitizationService) {}

  ngOnInit() {
    // There is no hovering/clicking so the default popup must be shown.
    if (this.data == null) {
      this.defaultPopup = true;
      return;
    }
    // It is the unaggregated view where each tablet has it's alias as a title.
    if (this.alias != null) {
      this.title = this.alias.cell + ' - ' + this.alias.uid;
    }
    // The map has been clicked so show full detailed popup.
    if (this.clicked) {
      if (this.alias != null) {
        this.parseUnaggregatedData();
        this.zone.run( () => { this.unaggregated = true; });
      } else {
        this.zone.run( () => { this.aggregated = true; });
      }
    }
  }

  getType(type: number) {
    if (type === 1) {
      return 'MASTER';
    }
    if (type === 2) {
      return 'REPLICA';
    }
    if (type === 3) {
      return 'RDONLY';
    }
  }

  // parseData gets the tabletStats and sets the proper values.
  parseUnaggregatedData() {
    this.tabletService.getTabletHealth(this.alias.cell, this.alias.uid).subscribe( health => {
      this.keyspace = health.Target.keyspace;
      this.shard = health.Target.shard;
      this.tabletType = this.getType(health.Target.tablet_type);
      this.hostname = health.Tablet.hostname;
      this.tabletUrl = this.sanitizer.bypassSecurityTrustResourceUrl(`http://${health.Tablet.hostname}:${health.Tablet.port_map.vt}`);
      this.lag = (typeof health.Stats.secondsBehindMaster === 'undefined') ? 0 : health.Stats.secondsBehindMaster;
      this.qps = (typeof health.Stats.qps === 'undefined') ? 0 : health.Stats.qps;
      this.serving = (typeof health.Serving === 'undefined') ? true : health.Serving;
      this.error = (typeof health.Stats.healthError === 'undefined') ? 'None' : health.Stats.healthError;
      this.lastError = (health.LastError == null) ? 'None' : health.LastError.string;
      this.zone.run(() => { this.healthDataReady = true; });
    });
  }
}
