import { Component } from '@angular/core';

import { DialogContent } from '../shared/dialog/dialog-content';

@Component({
  selector: 'vt-keyspace-extra',
  template:
    `
    <h1 class="vt-padding" *ngIf="dialogContent">
      Shard:  {{dialogContent.getParam('lower_bound')}}-{{dialogContent.getParam('upper_bound')}}
    </h1>
    `,
  styleUrls: ['../styles/vt.style.css'],
})

export class KeyspaceExtraComponent {
  title = 'Vitess Control Panel';
  dialogContent: DialogContent;
}
