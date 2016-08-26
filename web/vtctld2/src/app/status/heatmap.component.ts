import { Component, Input, AfterViewInit, NgZone, OnInit  } from '@angular/core';

import { TabletStatusService } from '../api/tablet-status.service';

declare var Plotly: any;

@Component({
  selector: 'vt-heatmap',
  templateUrl: './heatmap.component.html',
  styleUrls: ['./heatmap.component.css'],
})

export class HeatmapComponent implements AfterViewInit, OnInit {
  // heatmap is a heatmap struct equivalent (defined in go/vt/vtctld/tablet_stats_cache.go)
  @Input() heatmap: any;
  // metric needed to set the proper colorscale.
  @Input() metric: string;

  // data holds the values for the heatmap to display.
  data: number[][];
  // aliases holds the alias references for each datapoint in the heatmap.
  aliases: any[][];
  // yLabels is an array of objects each with one cell label and multiple type labels
  // each of which have a name and a rowspan
  // For example if there was 2 types within 1 cell yLabels would be like the following:
  // {CellName: {Name: 'Cell1', Rowspan: 2},
  //  TypeLabels: { {Name: 'REPLICA', Rowspan: 1}, {Name: 'RDONLY', Rowspan: 1}} }
  yLabels: Array<any>;
  // xLabels is an array with shard names as column labels.
  xLabels: Array<string>;
  // name is the keyspace name used as a unique ID for this heatmap.
  name: string;

  // Other variables needed to draw the heatmap.
  plotlyMap: any;
  first = true;
  heatmapHeight = 0;
  dataMin = 0;
  dataMax = 0;

  // colorscaleValue defines the gradient for the heatmap.
  private colorscaleValue;

  // Needed for the popup.
  showPopup = false;
  popupReady = false;
  popupTitle: string;
  popupData: Array<any>;
  private getRowHeight() { return 50; }
  private getXLabelsRowHeight() { return 25; }

  constructor(private zone: NgZone, private tabletService: TabletStatusService) { }

  // getTotalRows returns the number of rows the heatmap should span.
  getTotalRows() {
    if (this.heatmap.YLabels == null) {
      return this.heatmap.Data.length;
    }
    return this.heatmap.YLabels.reduce((prev, cur) => (prev + cur.Label.Rowspan), 0);
  }

  ngOnInit() {
    this.heatmapHeight = (this.getTotalRows() * this.getRowHeight() +
                          this.getXLabelsRowHeight());
    this.name = this.heatmap.Keyspace;
    this.data = this.heatmap.Data;
    this.aliases = this.heatmap.Aliases;
    this.yLabels = this.heatmap.YLabels;
    this.xLabels = this.heatmap.XLabels;
  }

  ngAfterViewInit() {
    this.drawHeatmap(this.metric);

    let elem = <any>(document.getElementById(this.name));
    elem.on('plotly_click', function(data){
      let x: number = data.points[0].x;
      let y: number = data.points[0].y;
      let alias = this.aliases[y][x];
      // TODO (pkulshre): Revise this to display the popup such that it doesn't disappear
      // when heatmap is refreshed during polling.
      this.tabletService.getTabletHealth(alias.cell, alias.uid).subscribe( health => {
        this.popupTitle = '' + alias.cell + '-' + alias.uid;
        this.popupData = health;
        this.popupReady = true;
        this.zone.run(() => { this.showPopup = true; });
      });
    }.bind(this));
    this.first = false;
  }

  closePopup() {
    this.zone.run(() => { this.showPopup = false; });
    this.popupReady = false;
  }

  // setupColorscale sets the right scale based on what metric the heatmap is displaying.
  setupColorscale(metric) {
    if (metric === 'healthy') {
      this.colorscaleValue = [
        [0.0, '#000000'],
        [0.33, '#F7EEDE'],
        [0.66, '#EA109A'],
        [1.0, '#A22417'],
      ];
      this.dataMin = 0;
      this.dataMax = 3;
    } else {
      let max = this.data.reduce((a, b) => a.concat(b))
                         .reduce((a, b) => (a > b) ? a : b);
      let percent = (max === 0) ? 1.0 : 1 / max;
      this.colorscaleValue = [
        [0.0, '#000000'],
        [percent, '#F7EEDE'],
        [1.0, '#A22417'],
      ];

      this.dataMin = -1;
      this.dataMax = max;
    }
  }

  drawHeatmap(metric) {
    this.setupColorscale(metric);

    // Settings for the Plotly heatmap.
    let chartInfo = [{
      z: this.data,
      zmin: this.dataMin,
      zmax: this.dataMax,
      x: this.xLabels,
      colorscale: this.colorscaleValue,
      type: 'heatmap',
      showscale: false
    }];

    let xAxisTemplate = {
      type: 'category',
      showgrid: false,
      zeroline: false,
      rangemode: 'nonnegative',
      side: 'top',
      ticks: '',
    };
    let yAxisTemplate = {
      showticklabels: false,
      ticks: '',
      fixedrange: true
    };
    let chartLayout = {
      xaxis: xAxisTemplate,
      yaxis: yAxisTemplate,
      margin: {
        t: 25,
        b: 0,
        r: 0,
        l: 0
      },
      showlegend: false,
    };

    Plotly.newPlot(this.name, chartInfo, chartLayout, {scrollZoom: true, displayModeBar: false});
  }
}
