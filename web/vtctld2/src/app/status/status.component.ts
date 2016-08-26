import { Component, OnInit, ComponentResolver, ViewChild, NgZone } from '@angular/core';
import { Router } from '@angular/router';

import { HeatmapComponent } from './heatmap.component.ts';
import { TabletStatusService } from '../api/tablet-status.service';
import { TopologyInfoService } from '../api/topology-info.service';

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

  // Needed for the construction of the heatmap.
  // heatmaps is an array of heatmap structs.
  private heatmaps: any;
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
  previousKeyspace: string;
  previousCell: string;
  previousType: string;
  previousMetric: string;

  // Needed to keep track of which data is being polled.
  statusService: any;

  constructor(private componentResolver: ComponentResolver, private tabletService: TabletStatusService,
              private router: Router, private zone: NgZone, private topoInfoService: TopologyInfoService) {}

  ngOnInit() {
    this.getBasicInfo();
  }

  // getTopologyInfo gets the keyspace, cell, tabletType, and metric information from the service. 
  getTopologyInfo() {
    this.topoInfoService.getCombinedTopologyInfo().subscribe( stream => {
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
        this.selectedKeyspace = params['keyspace'];
        this.selectedCell = params['cell'];
        this.selectedType = params['type'];
        this.selectedMetric = params['metric'];
        this.previousKeyspace = params['keyspace'];
        this.previousCell = params['cell'];
        this.previousType = params['type'];
        this.previousMetric = params['metric'];


        // Show default view if path was 'status/'
        if (this.selectedKeyspace == null && this.selectedCell == null && this.selectedType == null) {
          this.selectedKeyspace = 'all';
          this.selectedCell = 'all';
          this.selectedType = 'all';
          this.selectedMetric = 'healthy';
          this.previousKeyspace = 'all';
          this.previousCell = 'all';
          this.previousType = 'all';
          this.previousMetric = 'healthy';
          this.router.navigate(['/status'], this.getExtras());
        }

        this.zone.run(() => {
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
    if (this.previousKeyspace !== this.selectedKeyspace) {
      this.reroute();
    }
    this.previousKeyspace = this.selectedKeyspace;
  }

  handleCellChange(e) {
    if (this.previousCell !== this.selectedCell) {
      this.reroute();
    }
    this.previousCell = this.selectedCell;
  }

  handleTypeChange(e) {
    if (this.previousType !== this.selectedType) {
      this.reroute();
    }
    this.previousType = this.selectedType;
  }

  handleMetricChange(e) {
    if (this.previousMetric !== this.selectedMetric) {
      this.reroute();
    }
    this.previousMetric = this.selectedMetric;
  }

  reroute() {
    this.zone.run(() => {
     this.statusService.unsubscribe();
     this.heatmapDataReady = false;
     this.router.navigate(['/status'], this.getExtras());
   });
  }

  // Resets the heatmap data to new values when polling or when dropdown changes
  getHeatmapData() {
     // Subscribe to get updates every second.
     this.statusService = this.tabletService.getTabletStats(
         this.selectedKeyspace, this.selectedCell, this.selectedType,
         this.selectedMetric).subscribe(stats => {
           this.heatmaps = stats;
           this.metric = this.selectedMetric;
           this.heatmapDataReady = true;
         });
  }
}
