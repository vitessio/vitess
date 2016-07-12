import { Component,OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { MD_TOOLBAR_DIRECTIVES } from '@angular2-material/toolbar';
import { MD_LIST_DIRECTIVES } from '@angular2-material/list';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { BreadcrumbsComponent } from '../../shared/breadcrumbs.component';
import { TargetService } from '../../shared/targetService/target.service';
import { Tablet } from '../../shared/tabletObject/tablet';
import { RealtimeStats } from '../../shared/tabletObject/realtimeStats';
import { ROUTER_DIRECTIVES } from '@angular/router';

@Component({
  moduleId: module.id,
  selector: 'vt-target',
  templateUrl: './targetView.component.html',
  styleUrls: ['./targetView.component.css'],
  directives: [
    MD_LIST_DIRECTIVES,
    MD_TOOLBAR_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    BreadcrumbsComponent,
    ROUTER_DIRECTIVES,
  ],
  providers: [TargetService],
})

export class TargetViewComponent implements OnInit{
  title = 'Vitess Control Panel';

  private sub: any;
  
  tablets = [];
  breadcrumbs = [];
  
  path = "";

  cellName = "";
  keyspaceName = "";
  shardName = "";
  type = "";

  constructor(
     private targetService: TargetService,
     private router: Router
  ) {}


  ngOnInit() {
    this.getTablets();
    this.getBasicInfo();
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
        console.log( "Cell: " + this.cellName + 
                      "keyspace: " + this.keyspaceName + 
                      "shard: " + this.shardName +
                      "type: " + this.type);
      });
    this.breadcrumbs.push({ 
      name:"cell",
      queryParams: {cell: this.cellName}
    });
    this.breadcrumbs.push( {
      name: "keyspace",
      queryParams: {cell: this.cellName, keyspace: this.keyspaceName}
    });
    this.breadcrumbs.push({
      name: "shard",
      queryParams: {cell: this.cellName, keyspace: this.keyspaceName,
                    shard:this.shardName}
    });
    this.breadcrumbs.push({
      name: "type",
      queryParams: {cell: this.cellName, keyspace: this.keyspaceName,
                    shard:this.shardName, type:this.type}

    });
  }

  getTablets() {
    this.targetService.getTablets().subscribe( target => {
      console.log("GOT RESPONSE!!" + JSON.stringify(target));
      for (var index in  Object.keys((target))) {
        console.log("OBJ: " + JSON.stringify(target[index]));
        var listOfIds =  Object.keys(target[index]);
        for(var id in listOfIds) {
          console.log("id " + listOfIds[id] + " STATS" + JSON.stringify((target[index])[listOfIds[id]]));
          var stats = new RealtimeStats(target[index][listOfIds[id]]);
          var tempTab = new Tablet(this.cellName, listOfIds[id], this.keyspaceName, this.shardName, this.type, stats);
          console.log(JSON.stringify(tempTab.stats));
          (this.tablets).push(tempTab);
        }
      }
      console.log("AFTER: " + JSON.stringify(this.tablets));
    });
  }

  hasError(tab: Tablet) {
    console.log("VALUE OF KEY: " + JSON.stringify(tab));
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
