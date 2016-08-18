import { Component, Input, OnInit } from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';

@Component({
  moduleId: module.id,
  selector: 'vt-tablet',
    templateUrl: './tablet.component.html',
    styleUrls: ['./tablet.component.css'],
    directives: [
      CORE_DIRECTIVES,
    ]
})

export class TabletComponent implements OnInit {
  @Input() title: string;
  @Input() data;
  dataToDisplay: Array<any> = [];

  ngOnInit() {
    this.parseData();
  }

  // parseData goes through the input TabletStats object and stores it to display.
  parseData() {
    // TODO(pkulshre): test/update this when backend JSON encoder changed.
    this.dataToDisplay.push({ name: 'replication lag: ', value: this.data.Stats.secondsBehindMaster });
    this.dataToDisplay.push({ name: 'qps: ', value: this.data.Stats.qps });
    this.dataToDisplay.push({ name: 'Health Error: ', value: this.data.Stats.healthError });
  }
}
