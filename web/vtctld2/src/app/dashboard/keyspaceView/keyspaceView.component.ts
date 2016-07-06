import { Component, Input, OnInit, OnDestroy } from '@angular/core';
import { ROUTER_DIRECTIVES, ActivatedRoute, Router } from '@angular/router';
import { KeyspaceService } from '../../shared/keyspaceService/keyspace.service';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';

@Component({
  moduleId: module.id,
  selector: 'vt-keyspace-view',
  templateUrl: './keyspaceView.component.html',
  styleUrls: ['./keyspaceView.component.css'],
  directives: [
            ROUTER_DIRECTIVES,
            MD_CARD_DIRECTIVES,
            MD_BUTTON_DIRECTIVES],
  providers: [KeyspaceService],
})
export class KeyspaceViewComponent implements OnInit, OnDestroy{
  private sub: any;
  keyspaceName: string;
  keyspace = {};
  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private keyspaceService: KeyspaceService) {}

  ngOnInit() {
    this.sub = this.route.params.subscribe(params => {
       this.keyspaceName = params['keyspaceName']; 
       this.getKeyspace(this.keyspaceName);
     });
    
  }

  ngOnDestroy() {
    this.sub.unsubscribe();
  }

  getKeyspace(keyspaceName) {
    this.keyspaceService.getKeyspace(keyspaceName).subscribe((keyspace) => {
      console.log(keyspace[0]);
      this.keyspace = keyspace[0];
    });
  }

}
