import { Component, OnInit, ComponentResolver, ViewContainerRef } from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';
import { Component, OnInit, ComponentResolver, ViewChild, NgZone } from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';
import { Component, OnInit, ViewChild } from '@angular/core';

import { HeatmapComponent } from './heatmap.component';
import { TabletStatusService } from '../api/tablet-status.service';
import { TopologyInfoService } from '../api/topology-info.service';

import { Router } from '@angular/router';

import { Dropdown } from 'primeng/primeng';
import { SelectItem } from 'primeng/primeng';

@Component({
  selector: 'vt-status',
  templateUrl: './status.component.html',
  styleUrls: [],
})

export class StatusComponent implements OnInit {
  @ViewChild(HeatmapComponent) heatmap: HeatmapComponent;

  // Needed for the router.
  private sub: any;
  metricKey = 'metric';
  keyspaceKey = 'keyspace';
  cellKey = 'cell';
  typeKey = 'type';

  // Needed for the construction of the heatmap.
  private data: number[][];
  private aliases: any[][];
  // yLabels is an array of structs with the cell and array of tabletTypes.
  private yLabels: Array<any>;
  private xLabels: Array<string>;
  private metric: string;
  private heatmapDataReady: boolean = false;

  // Needed for the dropdowns.
  keyspaces: SelectItem[] = [];
  cells: SelectItem[] = [];
  tabletTypes: SelectItem[] = [];
  metrics: SelectItem[] = [];
  selectedKeyspace: string;
  selectedCell: string;
  selectedType: string;
  selectedMetric: string;

  // Needed to keep track of which data is being polled.
  statusService: any;

  constructor(private componentResolver: ComponentResolver, private tabletService: TabletStatusService,
              private router: Router, private zone: NgZone, private topoInfoService: TopologyInfoService) {}

  ngOnInit() {
    this.getBasicInfo();
  }

  // getTopologyInfo gets the keyspace, cell, tabletType, and metric information from the service. 
  getTopologyInfo() {
    this.topoInfoService.getKeyspacesAndCell().subscribe( stream => {
       let keyspacesReturned = stream[0];
       this.keyspaces = [];
       for (let i = 0; i < keyspacesReturned.length; i++) {
         let keyspace = keyspacesReturned[i];
         this.keyspaces.push({label: keyspace, value: keyspace});
       }
       this.keyspaces.push({label: 'all', value: 'all'});

       let cellsReturned = stream[1];
       this.cells = [];
       for (let i = 0; i < cellsReturned.length; i++) {
         let cell = cellsReturned[i];
         this.cells.push({label: cell, value: cell});
       }
       this.cells.push({label: 'all', value: 'all'});
    });

    this.tabletTypes = [];
    let tabletTypesReturned = this.topoInfoService.getTypes();
    for (let i = 0; i < tabletTypesReturned.length; i++) {
      let tabletType = tabletTypesReturned[i];
      this.tabletTypes.push({label: tabletType, value: tabletType});
    }
    this.tabletTypes.push({label: 'all', value: 'all'});

    this.metrics = [];
    let metricsReturned = this.topoInfoService.getMetrics();
    for (let i = 0; i < metricsReturned.length; i++) {
      let metric = metricsReturned[i];
      this.metrics.push({label: metric, value: metric});
    }
  }

  // getBasicInfo is responsible for populating the dropdowns based on the URL.
  getBasicInfo() {
    this.sub = this.router
      .routerState
      .queryParams
      .subscribe(params => {
        this.getTopologyInfo();

        // Setting the beginning values of each category. 
        this.selectedKeyspace = params[this.keyspaceKey];
        this.selectedCell = params[this.cellKey];
        this.selectedType = params[this.typeKey];
        this.selectedMetric = params[this.metricKey];

        // Show deafult view if path was 'status/'
        if (this.selectedKeyspace == null && this.selectedCell == null && this.selectedType == null) {
          this.selectedKeyspace = 'all';
          this.selectedCell = 'all';
          this.selectedType = 'all';
          this.selectedMetric = 'healthy';
          this.router.navigate(['/status'], this.getExtras());
        }

        this.zone.run( () => {
          this.getHeatmapData();
        });
    });
  }

  getExtras() {
    return {
      queryParams: { 'keyspace': this.selectedKeyspace,
                     'cell': this.selectedCell,
                     'type': this.selectedType,
                     'metric': this.selectedMetric},
    };
  }

  // The next four functions route to a new page once the dropdown is changed.
  handleKeyspaceChange(e) {
   this.zone.run( () => {
     this.statusService.unsubscribe();
     this.router.navigate(['/status'], this.getExtras());
   });
  }

  handleCellChange(e) {
   this.zone.run( () => {
     this.statusService.unsubscribe();
     this.router.navigate(['/status'], this.getExtras());
   });
  }

  handleTypeChange(e) {
   this.zone.run( () => {
     this.statusService.unsubscribe();
     this.router.navigate(['/status'], this.getExtras());
   });
  }

  handleMetricChange(e) {
   this.zone.run( () => {
     this.statusService.unsubscribe();
     this.router.navigate(['/status'], this.getExtras());
   });
  }

  // Resets the heatmap data to new values when polling or when dropdown changes
  getHeatmapData() {
     // Subscribe to get updates every second.
     if  (this.statusService !=  null) {
      this.statusService.unsubscribe();
     }
     this.statusService = this.tabletService.getTabletStats(this.selectedKeyspace,
       this.selectedCell, this.selectedType, this.selectedMetric)
       .subscribe(stats => {
       this.data = stats.Data;
       this.aliases = stats.Aliases;
       this.yLabels = stats.Labels;
       this.xLabels = [];
       this.metric = this.selectedMetric;
       for (let i = 0; i < stats.Data[0].length; i++) {
         this.xLabels.push('' + i);
       }
       this.heatmapDataReady = true;
     });
  }
}
