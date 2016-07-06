import { Component, Input, OnInit, OnDestroy } from '@angular/core';
import { ROUTER_DIRECTIVES, ActivatedRoute, Router } from '@angular/router';
import { KeyspaceService } from '../keyspaceService/keyspace.service';
import { TabletService } from '../tabletService/tablet.service';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';

@Component({
  moduleId: module.id,
  selector: 'vt-shard-view',
  templateUrl: './shardView.component.html',
  styleUrls: ['./shardView.component.css'],
  directives: [
            ROUTER_DIRECTIVES,
            MD_CARD_DIRECTIVES,
            MD_BUTTON_DIRECTIVES],
  providers: [
              KeyspaceService,
              TabletService],
})
export class ShardViewComponent implements OnInit, OnDestroy{
  private sub: any;
  keyspaceName: string;
  shardName: string;
  tablets = [];
  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private keyspaceService: KeyspaceService,
    private tabletService: TabletService) {}

  ngOnInit() {
    this.sub = this.route.params.subscribe(params => {
       this.keyspaceName = params['keyspaceName']; 
       this.shardName = params['shardName'];
       this.getTablets(this.keyspaceName, this.shardName);
     }); 
  }

  ngOnDestroy() {
    this.sub.unsubscribe();
  }

  getTablets(keyspaceName, shardName) {
    this.tabletService.getTablets(keyspaceName, shardName).subscribe((tablets) => {
      this.tablets = tablets;
    });
  }

  getName(type) {
    switch (type) {
      case 1:
        return "Master";
      case 2:
        return "Replica";
      case 3:
        return "Read Only";
      default:
        return "";
    }
  }
}
