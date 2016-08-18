import { Component, OnInit, ComponentResolver, ViewContainerRef } from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';

import { HeatmapComponent } from './heatmap.component';

@Component({
  selector: 'status',
  templateUrl: './status.component.html',
  styleUrls: [],
  directives: [
    CORE_DIRECTIVES,
    HeatmapComponent,
  ]
})

export class StatusComponent implements OnInit {

  private data: number[][];
  // yLabels is an array of structs with the cell and array of tabletTypes.
  private yLabels: Array<any>;
  private xLabels: Array<string>;
  private name: string;

  constructor(private componentResolver: ComponentResolver, private vcRef: ViewContainerRef) {}

  ngOnInit() {
    // TODO(pkulshre): Get data and labels from appropriate services.
  }
}
