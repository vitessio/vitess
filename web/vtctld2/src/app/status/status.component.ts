import { Component, OnInit, ComponentResolver, ViewContainerRef } from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';

import { PolymerElement } from '@vaadin/angular2-polymer';

import { HeatmapComponent } from './heatmap.component';

@Component({
  moduleId: module.id,
  selector: 'status',
  templateUrl: './status.component.html',
  styleUrls: [],
  directives: [
    CORE_DIRECTIVES,
    PolymerElement('paper-dropdown-menu'),
    PolymerElement('paper-listbox'),
    PolymerElement('paper-item'),
    HeatmapComponent
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
