import { Component,OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { MD_TOOLBAR_DIRECTIVES } from '@angular2-material/toolbar';
import { MD_LIST_DIRECTIVES } from '@angular2-material/list';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { BreadcrumbsComponent } from '../../shared/breadcrumbs.component';
import { TabletService } from '../../shared/tabletService/tablet.service';
import { Tablet } from '../../shared/tabletObject/tablet';
import { RealtimeStats } from '../../shared/tabletObject/realtimeStats';
import { ROUTER_DIRECTIVES } from '@angular/router';

@Component({
  moduleId: module.id,
  selector: 'vt-target',
  templateUrl: './tabletView.component.html',
  styleUrls: ['./tabletView.component.css'],
  directives: [
    MD_LIST_DIRECTIVES,
    MD_TOOLBAR_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    BreadcrumbsComponent,
    ROUTER_DIRECTIVES,
  ],
  providers: [TabletService],
})

export class TabletViewComponent implements OnInit{
  title = 'Tablets';

  private sub: any;

  tablets = [];
  breadcrumbs = [];

  cellName = "";
  keyspaceName = "";
  shardName = "";
  type = "";

  constructor(
     private tabletService: TabletService,
     private router: Router
  ) {}


  ngOnInit() {
    this.getTablets();
    this.getBasicInfo();
    this.setUpBreadcrumbs();
  }

  getBasicInfo() {
    this.sub = this.router
      .routerState
      .queryParams
      .subscribe(params => {
        this.cellName = params["cell"];
        this.keyspaceName = params["keyspace"];
        this.shardName = params["shard"];
        this.type = params["type"];
      });
  }

  setUpBreadcrumbs() {
    this.breadcrumbs.push({
      name:this.cellName,
      queryParams: {cell: this.cellName}
    });
    this.breadcrumbs.push( {
      name: this.keyspaceName,
      queryParams: {cell: this.cellName, keyspace: this.keyspaceName}
    });
    this.breadcrumbs.push({
      name: this.shardName,
      queryParams: {cell: this.cellName, keyspace: this.keyspaceName,
                    shard:this.shardName}
    });
    this.breadcrumbs.push({
      name: this.type,
      queryParams: {cell: this.cellName, keyspace: this.keyspaceName,
                    shard:this.shardName, type:this.type}

    });

  }

  getTablets() {
    this.tabletService.getTablets().subscribe( tablet_statuses => {
      for (var index in  Object.keys((tablet_statuses))) {
        var listOfIds =  Object.keys(tablet_statuses[index]);
        for(var id in listOfIds) {
          var stats = new RealtimeStats(tablet_statuses[index][listOfIds[id]]);
          var tempTab = new Tablet(this.cellName, listOfIds[id],
            this.keyspaceName, this.shardName, this.type, stats);
          (this.tablets).push(tempTab);
        }
      }
    });
  }

  hasError(tab: Tablet) {
    var error = tab.stats.HealthError;
    if(error === "" ) {
      return false;
    }
    return true;
  }

  getShardNumber(shardName: string) {
    if(shardName === "0") {
      return 0;
    }
    var parts = shardName.split("-");
    var lowerBound = parts[0];
    var upperBound = parts[1];
    if(lowerBound === "") {
      return 0;
    }
    if(upperBound === "") {
      var len = lowerBound.length;
      if(len == 1) { upperBound = "16"; }
      else if(len == 2) { upperBound = "256"; }
      else if(len == 3) { upperBound = "4096"; }
      else if(len == 4) { upperBound = "65536"; }
    }
    var interval = parseInt(upperBound) - parseInt(lowerBound);
    let num = parseInt(lowerBound)/interval;
    return num;
  }

  getLink (tab: Tablet) { 
    return "https://viceroy.corp.google.com/youtube_vitess/vttablet?dbtype=" +
            tab.type + "&mob=" + tab.keyspaceName + "&shard=" +
            this.getShardNumber(tab.shardName) + "&zone=" + tab.cell;
  }
}
