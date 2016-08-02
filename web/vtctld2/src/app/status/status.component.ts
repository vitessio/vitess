import { Component, AfterViewInit, ComponentResolver, ViewContainerRef } from '@angular/core';
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

export class StatusComponent implements AfterViewInit {

  constructor(private componentResolver: ComponentResolver, private vcRef: ViewContainerRef) {}

  ngAfterViewInit() {
      // Dynamically adding a heatmap component to the current view.
      this.componentResolver.resolveComponent(HeatmapComponent).then(factory => {
        let map = this.vcRef.createComponent(factory);
        //TODO(pkulshre): set input variables of the heatmap component.
      });
  }
}
