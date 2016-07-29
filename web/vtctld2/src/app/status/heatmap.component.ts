import { Component, Input, AfterViewInit} from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';
import { PolymerElement } from '@vaadin/angular2-polymer';

declare var Plotly: any;

@Component({
  moduleId: module.id,
  selector: 'heatmap-component',
    templateUrl: './heatmap.component.html',
    styleUrls: [],
    directives: [
      CORE_DIRECTIVES,
      MD_BUTTON_DIRECTIVES,
      PolymerElement('paper-dropdown-menu'),
      PolymerElement('paper-listbox'),
      PolymerElement('paper-item')
    ]
})

export class HeatmapComponent implements AfterViewInit {
  @Input() data: number[][];
  @Input() xLabels: Array<String>;
  @Input() yLabels: Array<String>;
  @Input() heatmapName: String;

  /* colorscaleValue defines the gradient for the heatmap */
  colorscaleValue = [
    [0.0, '#17A234'],
    [0.5, '#A22417'],
    [1.0, '#424141'],
  ];

  ngAfterViewInit() {
    this.drawHeatmap();
  }

  drawHeatmap() {
     let chartInfo = [{
       z: this.data,
       x: this.xLabels,
       y: this.yLabels,
       colorscale: this.colorscaleValue,
       type: 'heatmap',
       showscale: false
     }];

     let xAxisTemplate = {
       showgrid: false,
       zeroline: false,
       side: 'top',
       ticks: ''
     };
     let yAxisTemplate = {
       fixedrange: true
     };
     let chartLayout = {
       xaxis: yAxisTemplate,
       yaxis: xAxisTemplate,
       showlegend: false,
    };

    Plotly.newPlot(this.heatmapName, chartInfo, chartLayout, {scrollZoom: true, displayModeBar: false});
  }
}
