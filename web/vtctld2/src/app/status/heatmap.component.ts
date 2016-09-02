import { Component, Input, AfterViewInit, NgZone, OnInit } from '@angular/core';

import { TabletStatusService } from '../api/tablet-status.service';

declare var Plotly: any;

@Component({
  selector: 'vt-heatmap',
  templateUrl: './heatmap.component.html',
  styleUrls: ['./heatmap.component.css'],
})

export class HeatmapComponent implements AfterViewInit, OnInit {
  // heatmap is a heatmap struct equivalent (defined in go/vt/vtctld/tablet_stats_cache.go).
  @Input() heatmap: any;
  // metric needed to set the proper colorscale.
  @Input() metric: string;
  // name is the keyspace name as well as the name for this plotly div.
  @Input() name: string;

  // data holds the values for the heatmap to display.
  data: number[][];
  // aliases holds the alias references for each datapoint in the heatmap.
  aliases: any[][];
  // Keyspace is a label object with the keyspace name and its rowspan.
  keyspace: any;
  // yLabels is an array of objects each with one cell label and multiple type labels
  // each of which have a name and a rowspan
  // For example if there was 2 types within 1 cell yLabels would be like the following:
  // {CellName: {Name: 'Cell1', Rowspan: 2},
  //  TypeLabels: { {Name: 'REPLICA', Rowspan: 1}, {Name: 'RDONLY', Rowspan: 1}} }
  yLabels: Array<any>;
  // xLabels is an array with shard names as column labels.
  xLabels: Array<string>;
  // yGrid stores the info for the horizontal grid lines.
  yGrid: Array<number>;

  // Other variables needed to draw the heatmap.
  plotlyMap: any;
  heatmapHeight = 0;
  heatmapWidth = 0;
  dataMin = 0;
  dataMax = 0;

  // colorscaleValue defines the gradient for the heatmap.
  private colorscaleValue;

  // Needed for the popup.
  showPopup = true;
  popupTitle: string;
  popupAlias: any;
  popupData: Array<any>;
  popupKeyspace: string;
  popupShard: string;
  popupCell: string;
  popupTabletType: string;
  popupClickState: boolean;
  clicked = false;

  private getRowHeight() { return 20; }
  private getXLabelsRowHeight() { return 50; }

  constructor(private zone: NgZone, private tabletService: TabletStatusService) { }

  // getTotalRows returns the number of rows the heatmap should span.
  getTotalRows() {
    return this.heatmap.KeyspaceLabel.Rowspan;
  }

  // getRemainingRows returns a list of the numbers from 1 to upperbound.
  // It is used by the HTML template to get the number of empty rows needed to properly
  // have a column spanning additional rows.
  getRemainingRows(upperBound) {
    let numbers = [];
    for (let i = 1; i < upperBound; i++) {
      numbers.push(i);
    }
    return numbers;
  }

  ngOnInit() {
    this.heatmapHeight = (this.getTotalRows() * this.getRowHeight() +
                          this.getXLabelsRowHeight());
    this.data = this.heatmap.Data;
    this.aliases = this.heatmap.Aliases;
    this.xLabels = this.heatmap.ShardLabels;
    this.yLabels = this.heatmap.CellAndTypeLabels;
    this.keyspace = this.heatmap.KeyspaceLabel;
    this.yGrid = this.heatmap.YGridLines;

    let heatmapTable = document.getElementById('heatmapTable');
    this.heatmapWidth = heatmapTable.clientWidth < 10 ? 10 : heatmapTable.clientWidth;
  }

  getCell(rowClicked) {
    let sum = 0;
    for (let i = this.yLabels.length - 1; i >= 0; i--) {
      sum += this.yLabels[i].CellLabel.Rowspan;
      if (rowClicked < sum) {
        return this.yLabels[i].CellLabel.Name;
      }
    }
    return '';
  }

  getType(rowClicked) {
    let sum = 0;
    for (let cellIndex = this.yLabels.length - 1; cellIndex >= 0; cellIndex--) {
      if (this.yLabels[cellIndex].TypeLabels == null) {
        return 'all';
      }
      for ( let typeIndex = this.yLabels[cellIndex].TypeLabels.length - 1; typeIndex >= 0; typeIndex--) {
        sum += this.yLabels[cellIndex].TypeLabels[typeIndex].Rowspan;
        if (rowClicked < sum) {
          return this.yLabels[cellIndex].TypeLabels[typeIndex].Name;
        }
      }
    }
    return '';
  }

  ngAfterViewInit() {
    this.drawHeatmap(this.metric);
    let elem = <any>(document.getElementById(this.name));

    // onClick handler for the heatmap.
    // Upon clicking, the popup will display all relevent data for that data point.
    elem.on('plotly_click', function(data) {
      this.zone.run(() => { this.showPopup = false; });
      let shardIndex = this.xLabels.indexOf(data.points[0].x);
      let rowIndex = data.points[0].y;
      if (this.aliases == null) {
        this.popupAlias = null;
      } else {
        this.popupAlias = this.aliases[rowIndex][shardIndex];
      }
      this.popupData = data.points[0].z;
      this.popupKeyspace = this.keyspace.Name;
      this.popupShard = data.points[0].x;
      this.popupCell = this.getCell(rowIndex);
      this.popupType = this.getType(rowIndex);
      this.popupClickState = true;
      this.zone.run(() => { this.showPopup = true; });
      this.clicked = true;
    }.bind(this));

    // onHover handler for the heatmap.
    // Upon hover, the popup will display basic data for that data point
    // (tablet alias - if applicable, keyspace, cell, shard, and type)
    elem.on('plotly_hover', function(data) {
      if (this.clicked === true) {
        return;
      }
      this.zone.run(() => { this.showPopup = false; });
      let shardIndex = this.xLabels.indexOf(data.points[0].x);
      let rowIndex = data.points[0].y;
      if (this.aliases == null) {
        this.popupAlias = null;
      } else {
        this.popupAlias = this.aliases[rowIndex][shardIndex];
      }
      this.popupData = data.points[0].z;
      this.popupKeyspace = this.keyspace.Name;
      this.popupShard = data.points[0].x;
      this.popupCell = this.getCell(rowIndex);
      this.popupType = this.getType(rowIndex);
      this.popupClickState = false;
      this.zone.run(() => { this.showPopup = true; });
    }.bind(this));
  }

  // closePopup removes the popup from the view.
  closePopup() {
    this.zone.run(() => { this.showPopup = false; });
    this.clicked = false;
  }

  // redraw updates the existing map with new data.
  redraw(stats, metric) {
    this.data = stats.Data;
    this.aliases = stats.Aliases;
    this.yLabels = stats.CellAndTypeLabels;
    this.xLabels = stats.ShardLabels;
    this.name = stats.KeyspaceLabel.Name;
    this.yGrid = stats.YGridLines;
    // Settings for the Plotly heatmap.
    let chartInfo = {
      z: [this.data],
      x: [this.xLabels],
    };
    let chartLayout = {
      yaxis: { tickvals: [this.yGrid] },
    };
    Plotly.restyle(this.name, chartInfo, chartLayout, [0]);
  }

  // setupColorscale sets the right scale based on what metric the heatmap is displaying.
  setupColorscale(metric) {
    if (metric === 'health') {
      this.colorscaleValue = [
        [0.0, '#000000'],
        [0.25, '#FFFFFF'],
        [0.5, '#F8CFE9'],
        [0.75, '#EA109A'],
        [1.0, '#A22417'],
      ];
      this.dataMin = -1;
      this.dataMax = 3;
    } else {
      // The max value describes the highest value of lag or qps present in the current data.
      let max = this.data.reduce((a, b) => a.concat(b))
                         .reduce((a, b) => (a > b) ? a : b);
      let percent = (max === 0) ? 1.0 : 1 / (1 + max);
      this.colorscaleValue = [
        [0.0, '#000000'],
        [percent, '#FFFFFF'],
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
      showscale: false,
      hoverinfo: 'none'
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
      tickmode: 'array',
      ticks: 'inside',
      ticklen: this.heatmapWidth,
      tickcolor: '#000',
      tickvals: this.yGrid,
      fixedrange: true
    };
    let chartLayout = {
      xaxis: xAxisTemplate,
      yaxis: yAxisTemplate,
      width: this.heatmapWidth,
      height: this.heatmapHeight,
      margin: {
        t: this.getXLabelsRowHeight(),
        b: 0,
        r: 0,
        l: 0
      },
      showlegend: false,
    };
    Plotly.newPlot(this.name, chartInfo, chartLayout, {scrollZoom: true, displayModeBar: false});
  }
}
