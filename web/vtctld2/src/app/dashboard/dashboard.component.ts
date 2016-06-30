import { Component, OnInit} from '@angular/core';
import { ROUTER_DIRECTIVES } from '@angular/router';
import { KeyspaceService } from '../keyspaceService/keyspace.service';
@Component({
  moduleId: module.id,
  selector: 'vt-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.css'],
  directives: [ROUTER_DIRECTIVES],
  providers: [KeyspaceService],
})
export class DashboardComponent implements OnInit{
  title = 'Vitess Control Panel';
  keyspaces: string[];
  constructor(private keyspaceService: KeyspaceService) { }

  ngOnInit() {
    this.listKeyspaces();
  }

  listKeyspaces() {
    return this.keyspaceService.listKeyspaces().then(keyspaces => this.keyspaces = keyspaces);
  }
  
  healthy(keyspaceName: string) {
    return this.keyspaceService.healthy(keyspaceName).then(function(health){
      console.log(keyspaceName, " Healthy? ", health);
      return health;
    });
  }
}
