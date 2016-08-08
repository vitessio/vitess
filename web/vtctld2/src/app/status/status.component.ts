import { Component, OnInit, ComponentResolver, ViewContainerRef } from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';

import { PolymerElement } from '@vaadin/angular2-polymer';

import { HeatmapComponent } from './heatmap.component';
import { TabletService } from '../api/tablet.service';

@Component({
  moduleId: module.id,
  selector: 'status',
  templateUrl: './status.component.html',
  styleUrls: [],
  directives: [
    CORE_DIRECTIVES,
    PolymerElement('paper-dropdown-menu'),
    PolymerElement('paper-listbox'),
    PolymerElement('paper-item'),
    HeatmapComponent
  ],
  providers: [
    TabletService
  ]
})

export class StatusComponent implements OnInit {
  private data: number[][];
  // yLabels is an array of structs with the cell and array of tabletTypes.
  private yLabels: Array<any>;
  private xLabels: Array<string>;
  private name: string;
  private heatmapDataReady: boolean = false;

  constructor(private componentResolver: ComponentResolver, private vcRef: ViewContainerRef,
    private tabletService: TabletService) {}

  ngOnInit() {
     this.getHeatmapData();
  }

  getHeatmapData() {
    this.tabletService.getTabletStats('lag', 'test', 'test_keyspace', 'REPLICA').subscribe(stats => {
      this.data = stats.HeatmapData;
      this.yLabels = stats.HeatmapLabels;
      this.xLabels = [];
      for (let i = 0; i < stats.HeatmapData[0].length; i++) {
        this.xLabels.push('' + i);
      }
      this.name = 'heatmap';
      this.heatmapDataReady = true;
    });
  }
}
