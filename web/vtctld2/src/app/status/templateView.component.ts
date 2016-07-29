import { Component, OnInit } from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';
import { PolymerElement } from '@vaadin/angular2-polymer';

import { HeatmapComponent } from './heatmap.component';

@Component({
  moduleId: module.id,
  selector: 'template-view',
  templateUrl: './templateView.component.html',
  styleUrls: [],
  directives: [
    CORE_DIRECTIVES,
    MD_BUTTON_DIRECTIVES,
    PolymerElement('paper-dropdown-menu'),
    PolymerElement('paper-listbox'),
    PolymerElement('paper-item'),
    HeatmapComponent
  ]
})

export class TemplateComponent implements OnInit {
  /* Needed to create a heatmap */
  public data: number[][];
  public xLabels: Array<String>;
  public yLabels: Array<String>;
  public heatmapName: String;

  /* Needed for the three dropdown menus */
  public keyspaces: Array<String>;
  public cells: Array<String>;
  public tabletTypes: Array<String>;

  ngOnInit() {
    /* TODO(pkulshre) implement service to obtain data in this method */
    /* TODO(pkulshre) implement services to obtain keyspace, cell, type information */
  }
}
