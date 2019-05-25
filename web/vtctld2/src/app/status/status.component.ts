import { Component, OnInit, ComponentResolver, ViewChildren, NgZone, QueryList, OnDestroy } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { HeatmapComponent } from './heatmap.component';
import { TabletStatusService } from '../api/tablet-status.service';
import { TopologyInfoService } from '../api/topology-info.service';

import { SelectItem } from 'primeng/primeng';

@Component({
  selector: 'vt-status',
  templateUrl: './status.component.html',
  styleUrls: ['./status.component.css'],
})

export class StatusComponent implements OnInit, OnDestroy {
  @ViewChildren(HeatmapComponent) heatmaps: QueryList<HeatmapComponent>;

  // Needed for the router.
  private sub: any;

  // Needed for the construction of the heatmap.
  private heatmapDataReady: boolean = false;
  // listOfKeyspaces is an array of strings representing keyspace names.
  listOfKeyspaces: Array<string> = [];
  // mapOfKeyspaces is a mapping between a keyspace and its heatmap struct, which has
  // all the information to construct a heatmap such as data, aliases, and labels.
  mapOfKeyspaces: { [key: string]: any; } = {};

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

  constructor(private tabletService: TabletStatusService,
              private router: Router, private route: ActivatedRoute, private zone: NgZone,
              private topoInfoService: TopologyInfoService) {}

  ngOnInit() {
    this.getBasicInfo();
  }

  ngOnDestroy() {
    this.sub.unsubscribe();
  }

  // getTopologyInfo gets the keyspace, cell, tabletType, and metric information from the service.
  getTopologyInfo(selectedKeyspace, selectedCell) {
    this.topoInfoService.getCombinedTopologyInfo(selectedKeyspace, selectedCell).subscribe( data => {
      let keyspacesReturned = data.Keyspaces;
      this.keyspaces = [];
      this.keyspaces.push({label: 'all', value: 'all'});
      for (let i = 0; i < keyspacesReturned.length; i++) {
        let keyspace = keyspacesReturned[i];
        this.keyspaces.push({label: keyspace, value: keyspace});
      }

      let cellsReturned = data.Cells;
      this.cells = [];
      this.cells.push({label: 'all', value: 'all'});
      for (let i = 0; i < cellsReturned.length; i++) {
        let cell = cellsReturned[i];
        this.cells.push({label: cell, value: cell});
      }

      let tabletTypesReturned = data.TabletTypes;
      this.tabletTypes = [];
      this.tabletTypes.push({label: 'all', value: 'all'});
      for (let i = 0; i < tabletTypesReturned.length; i++) {
        let tabletType = tabletTypesReturned[i];
        this.tabletTypes.push({label: tabletType, value: tabletType});
      }
    });
    this.metrics = [];
    let metricsReturned = this.topoInfoService.getMetrics();
    for (let i = 0; i < metricsReturned.length; i++) {
      let metric = metricsReturned[i];
      this.metrics.push({label: metric, value: metric});
    }
  }

  // getBasicInfo is responsible for populating the dropdowns based on the URL.
  getBasicInfo() {
    this.sub = this.route.queryParams.subscribe(params => {
        // Setting the beginning values of each category.
        this.selectedKeyspace = params['keyspace'];
        this.selectedCell = params['cell'];
        this.selectedType = params['type'];
        this.selectedMetric = params['metric'];
        this.previousKeyspace = params['keyspace'];
        this.previousCell = params['cell'];
        this.previousType = params['type'];
        this.previousMetric = params['metric'];

        this.getTopologyInfo(this.selectedKeyspace, this.selectedCell);
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
     this.heatmapDataReady = false;
     this.listOfKeyspaces = [];
     this.mapOfKeyspaces = {};
     this.router.navigate(['/status'], this.getExtras());
   });
  }

  // equals checks the equality of 2 arrays.
  equals(arr1, arr2) {
    return arr1.length === arr2.length && arr1.every((elem1, elem2) => elem1 === arr2[elem2]);
  }

  updateKeyspaces(stats) {
    let newKeyspaces = [];
    for (let i = 0; i < stats.length; i++) {
      let keyspaceName = stats[i].KeyspaceLabel.Name;
      newKeyspaces.push(keyspaceName);
      this.mapOfKeyspaces[keyspaceName] = Object.assign({}, stats[i]);
    }
    if (!this.equals(newKeyspaces, this.listOfKeyspaces)) {
      this.listOfKeyspaces = newKeyspaces;
      return true;
    }
    return false;
  }

  // Resets the heatmap data to new values when polling or when dropdown changes
  getHeatmapData() {
     // Unsubscribe to the previous observable since it was probably from a different view.
     if (this.statusService != null) {
       this.statusService.unsubscribe();
     }
     // Subscribe to get updates every second.
     this.statusService = this.tabletService.getTabletStats(this.selectedKeyspace,
         this.selectedCell, this.selectedType,
         this.selectedMetric).subscribe(stats => {
           let wasChanged = this.updateKeyspaces(stats);
           // If there is no change in the keyspaces, then we will update the map already created.
           if (!wasChanged) {
             this.heatmaps.toArray().forEach((heatmap) => heatmap.redraw(this.mapOfKeyspaces[heatmap.name], this.selectedMetric));
           }
           this.heatmapDataReady = true;
         });
  }
}
