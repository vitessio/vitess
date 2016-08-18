import { Component, OnInit, ViewChild } from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';

import { HeatmapComponent } from './heatmap.component';
import { TabletStatusService } from '../api/tablet-status.service';

@Component({
  selector: 'vt-status',
  templateUrl: './status.component.html',
  styleUrls: [],
  directives: [
    CORE_DIRECTIVES,
    HeatmapComponent
  ],
  providers: [
    TabletStatusService
  ]
})

export class StatusComponent implements OnInit {
  @ViewChild(HeatmapComponent) heatmap: HeatmapComponent;

  // Used for the heatmap component.
  private data: number[][];
  private aliases: any[][];
  // yLabels is an array of structs with the cell and array of tabletTypes.
  private yLabels: Array<any>;
  private xLabels: Array<string>;
  private heatmapDataReady: boolean = false;

  constructor (private tabletService: TabletStatusService) {}

  ngOnInit() {
     this.getHeatmapData();
  }

  getHeatmapData() {
    // Subscribe to get updates every second.
    this.tabletService.getTabletStats('lag', 'test', 'test_keyspace', 'REPLICA').subscribe(stats => {
      this.data = stats.Data;
      this.aliases = stats.Aliases;
      this.yLabels = stats.Labels;
      this.xLabels = [];
      for (let i = 0; i < stats.Data[0].length; i++) {
        this.xLabels.push('' + i);
      }
      this.heatmapDataReady = true;

      // The heatmap has already been instantiated so it needs to be redrawn.
      if (this.heatmap != null) {
        this.heatmap.data = this.data;
        this.heatmap.aliases = this.aliases;
        this.heatmap.yLabels = this.yLabels;
        this.heatmap.xLabels = this.xLabels;
        this.heatmap.drawHeatmap();
      }
    });
  }
}
