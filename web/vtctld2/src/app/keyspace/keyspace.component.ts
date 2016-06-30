import { Component, Input, OnInit, OnDestroy } from '@angular/core';
import { ROUTER_DIRECTIVES, ActivatedRoute, Router } from '@angular/router';

@Component({
  moduleId: module.id,
  selector: 'vt-keyspace',
  templateUrl: './keyspace.component.html',
  styleUrls: ['./keyspace.component.css'],
  directives: [ROUTER_DIRECTIVES],
})
export class KeyspaceComponent implements OnInit, OnDestroy{
  private sub: any;
  keyspaceName: string;
  constructor(
    private route: ActivatedRoute,
    private router: Router) {}

  ngOnInit() {
    this.sub = this.route.params.subscribe(params => {
       this.keyspaceName = params['keyspaceName']; // (+) converts string 'id' to a number
     });
  }

  ngOnDestroy() {
    this.sub.unsubscribe();
  }

  title = 'Vitess Control Panel';
  
  @Input()
  keyspace: any;
  
  listKeyspaces = function() {
    return Object.keys(this.keyspaces);
  };
  healthy = function(keyspace) {
    return this.keyspaces[keyspace].healthy;
  }
  active = function(keyspace) {
    return this.keyspaces[keyspace].active;
  }
}
