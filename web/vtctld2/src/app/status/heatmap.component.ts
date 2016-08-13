import { Component, Input, AfterViewInit, OnInit } from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';

declare var Plotly: any;

@Component({
  moduleId: module.id,
  selector: 'vt-heatmap',
    templateUrl: './heatmap.component.html',
    styleUrls: ['./heatmap.component.css'],
    directives: [
      CORE_DIRECTIVES,
    ]
})

export class HeatmapComponent implements AfterViewInit, OnInit {
  @Input() data: number[][];
  @Input() aliases: any[][];
  // yLabels is an array of objects with 2 properties: the cell and array of tabletTypes.
  @Input() yLabels: Array<any>;
  @Input() xLabels: Array<string>;
  name: string;

  plotlyMap: any;

  // colorscaleValue defines the gradient for the heatmap.
  private colorscaleValue = [
    [0.0, '#424141'],
    [0.5, '#17A234'],
    [1.0, '#A22417'],
  ];
  private getRowHeight() { return 50; }
  private getXLabelsRowHeight() { return 25; }

  constructor(  ) { }

  // getTotalRows returns the size of the entire heatmap.
  getTotalRows() {
    let height = 0;
    for (let yLabel of this.yLabels) {
      height += yLabel.Label.Rowspan;
    }
    return height;
  }

  ngOnInit() {
    this.name = 'heatmap';
  }

  ngAfterViewInit() {
    this.drawHeatmap();

    let elem = <any>(document.getElementById(this.name));
    elem.on('plotly_click', function(data){
      // TODO(pkulshre): get tabletInfo from service.
     }.bind(this));
   }

  drawHeatmap() {
     // Settings for the Plotly heatmap.
     let chartInfo = [{
       z: this.data,
       zmin: -10,
       zmax: 10,
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
       height: (this.getTotalRows() * this.getRowHeight() + this.getXLabelsRowHeight()),
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
