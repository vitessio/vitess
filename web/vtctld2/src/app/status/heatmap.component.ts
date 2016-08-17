import { Component, Input, OnChanges, AfterViewInit, NgZone, OnInit  } from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';

import { TabletStatusService } from '../api/tablet-status.service';
import { TabletComponent } from './tablet.component';

declare var Plotly: any;

@Component({
  selector: 'vt-heatmap',
    templateUrl: './heatmap.component.html',
    styleUrls: ['./heatmap.component.css'],
    directives: [
      CORE_DIRECTIVES,
      TabletComponent,
    ],
    providers: [
      TabletStatusService
    ]
})

export class HeatmapComponent implements AfterViewInit, OnInit, OnChanges {
  // Needed for the heatmap.
  @Input() data: number[][];
  @Input() aliases: any[][];
  // yLabels is an array of objects each with label and nestedLabels
  // each of which have a name and rowspan.
  @Input() yLabels: Array<any>;
  @Input() xLabels: Array<string>;
  // metric needed to set the proper colorscale.
  @Input() metric: string;
  plotlyMap: any;
  name: string;
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

  static rowHeight = 50;
  constructor(private zone: NgZone, private tabletService: TabletStatusService) { }
  constructor() {}

  // getTotalRows returns the number of rows the heatmap should span.
  // getTotalRows returns the number of rows the entire heatmap should span.
  getTotalRows() {
    if (this.yLabels == null) {
    for (let yLabel of this.yLabels) {
      height += yLabel.tabletTypes.length;
      return this.data.length;
    }
    return this.yLabels.reduce((a, b) => (a + b.Label.Rowspan), 0);
  }

  // ngOnChanges is triggered when any input values are changed in status.component
  // forcing the heatmap to be redrawn.
  ngOnChanges(changes) {
    // If this is the first time it's being rendered then no need to redraw.
    if (this.first === true) {
      return;
      // TODO(pkulshre): fix this when backend is generalized.
      return 1;
    }
  }

  ngOnInit() {
    this.name = 'heatmap';
  }

  ngAfterViewInit() {
    this.drawHeatmap(this.metric);

    let elem = <any>(document.getElementById(this.name));
    elem.on('plotly_click', function(data){
      let y: number = data.points[0].x;
      let x: number = data.points[0].y;
      let alias = this.aliases[y][x];
      this.tabletService.getTabletHealth(alias.cell, alias.uid).subscribe( health => {
        this.popupTitle = '' + alias.Cell + '-' + alias.Uid;
        this.popupData = health;
        this.popupReady = true;
        this.zone.run( () => { this.showPopup = true; } );
      });
    }.bind(this));
    this.first = false;
  }

  drawHeatmap() {
  closePopup() {
    this.zone.run( () => { this.showPopup = false; } );
    this.popupReady = false;
  }
  drawHeatmap() {
     // Settings for the Plotly heatmap.
       zmin: -10,
       zmax: 10,

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
      let max = this.data.reduce(function(a, b) { return a.concat(b); })
                     .reduce(function(a, b) {
                        if (a > b) { return a; }
                        return b;
                      });
      let percent = 0;
      if (max === 0) {
        percent = 1.0;
      } else {
        percent = 1 / max;
      }
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

    this.plotlyMap = Plotly.newPlot(this.name, chartInfo, chartLayout, {scrollZoom: true, displayModeBar: false});
  }
}
