import { Component, Input, OnInit, OnDestroy } from '@angular/core';
import { ROUTER_DIRECTIVES, ActivatedRoute, Router } from '@angular/router';
import { TabletService } from '../../shared/tabletService/tablet.service';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';
import {DataTable} from 'primeng/primeng';
import {Column} from 'primeng/primeng';

@Component({
  moduleId: module.id,
  selector: 'vt-shard-view',
  templateUrl: './shardView.component.html',
  styleUrls: ['./shardView.component.css'],
  directives: [
            ROUTER_DIRECTIVES,
            MD_CARD_DIRECTIVES,
            MD_BUTTON_DIRECTIVES,
            DataTable,
            Column],
  providers: [
              TabletService],
})
export class ShardViewComponent implements OnInit, OnDestroy{
  private routeSub: any;
  keyspaceName: string;
  shardName: string;
  tablets = [];
  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private tabletService: TabletService) {}

  ngOnInit() {
    this.routeSub = this.router.routerState.queryParams
      .subscribe(params => {
        this.keyspaceName = params['keyspace'];
        this.shardName = params['shard'];
        this.getTablets(this.keyspaceName, this.shardName);
      }
    );
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
  }

  getTablets(keyspaceName, shardName) {
    this.tabletService.getTablets(keyspaceName, shardName).subscribe((tablets) => {
      console.log(tablets[0]);
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
