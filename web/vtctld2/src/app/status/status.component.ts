import { Component } from '@angular/core';
import { MD_TOOLBAR_DIRECTIVES } from '@angular2-material/toolbar';
import { MD_LIST_DIRECTIVES } from '@angular2-material/list';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { BreadcrumbsComponent } from '../shared/breadcrumbs.component';
import { ROUTER_DIRECTIVES } from '@angular/router';

@Component({
  moduleId: module.id,
  selector: 'vt-status',
  templateUrl: './status.component.html',
  styleUrls: ['./status.component.css'],
  directives: [
    MD_LIST_DIRECTIVES,
    MD_TOOLBAR_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    BreadcrumbsComponent,
    ROUTER_DIRECTIVES,
  ]
})
export class StatusComponent {
  title = 'Vitess Control Panel';

  cellName = "cell1"
  keyspaceName = "keyspace1";
  shardName = "0";
  type = "replica";
}

