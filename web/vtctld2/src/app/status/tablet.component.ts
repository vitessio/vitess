import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { ROUTER_DIRECTIVES } from '@angular/router';

import { MD_TOOLBAR_DIRECTIVES } from '@angular2-material/toolbar';
import { MD_LIST_DIRECTIVES } from '@angular2-material/list';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';

import { BreadcrumbsComponent } from '../shared/breadcrumbs.component';
import { Tablet } from '../api/tablet';
import { TabletService } from '../api/tablet.service';

@Component({
  moduleId: module.id,
  selector: 'vt-target',
  templateUrl: './tablet.component.html',
  styleUrls: ['./tablet.component.css'],
  directives: [
    MD_LIST_DIRECTIVES,
    MD_TOOLBAR_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    BreadcrumbsComponent,
    ROUTER_DIRECTIVES,
  ],
  providers: [TabletService],
})

export class TabletStatusComponent implements OnInit {
  title = 'Tablets';

  private sub: any;

  tablets = [];
  breadcrumbs = [];

  cellName = '';
  keyspaceName = '';
  shardName = '';
  type = '';

  constructor(
     private tabletService: TabletService,
     private router: Router
  ) {}


  ngOnInit() {
    this.getBasicInfo();
    this.getTablets();
    this.setUpBreadcrumbs();
  }

  getBasicInfo() {
    this.sub = this.router
      .routerState
      .queryParams
      .subscribe(params => {
        this.cellName = params['cell'];
        this.keyspaceName = params['keyspace'];
        this.shardName = params['shard'];
        this.type = params['type'];
      });
  }

  setUpBreadcrumbs() {
    this.breadcrumbs.push({
      name: this.cellName,
      queryParams: {cell: this.cellName}
    });
    this.breadcrumbs.push( {
      name: this.keyspaceName,
      queryParams: {cell: this.cellName, keyspace: this.keyspaceName}
    });
    this.breadcrumbs.push({
      name: this.shardName,
      queryParams: {cell: this.cellName, keyspace: this.keyspaceName, shard: this.shardName}
    });
    this.breadcrumbs.push({
      name: this.type,
      queryParams: {cell: this.cellName, keyspace: this.keyspaceName, shard: this.shardName, type: this.type}

    });

  }

  getTablets() {
    this.tabletService.getTablets(this.cellName, this.keyspaceName, this.shardName, this.type).subscribe( tablet_statuses => {
      for (let index of  Object.keys((tablet_statuses))) {
        let listOfIds = Object.keys (tablet_statuses[index]);
        for (let id of listOfIds) {
         let stats = Object.assign({}, tablet_statuses[index][listOfIds[id]]);
          let tempTab = new Tablet(this.cellName, listOfIds[id],
            this.keyspaceName, this.shardName, this.type, stats);
          (this.tablets).push(tempTab);
        }
      }
    });
  }

  hasError(tab: Tablet) {
    let error = tab.stats.HealthError;
    if (error === '' ) {
      return false;
    }
    return true;
  }
}
