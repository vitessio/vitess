import { Component } from '@angular/core';

import { DialogContent } from '../shared/dialog/dialogContent';

@Component({
  moduleId: module.id,
  selector: 'vt-keyspace-extra',
  template:
    `
    <h1 class="vt-padding" *ngIf="dialogContent">{{dialogContent.getParam('lowerBound')}}-{{dialogContent.getParam('upperBound')}}</h1>
    `,
  styleUrls: ['../styles/vt.style.css'],
})

export class KeyspaceExtraComponent {
  title = 'Vitess Control Panel';
  dialogContent: DialogContent;
}
