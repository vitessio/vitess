import { Component, OnInit, OnDestroy } from '@angular/core';
import { ROUTER_DIRECTIVES, ActivatedRoute, Router } from '@angular/router';
import { TabletService } from '../api/tablet.service';
import { DataTable, Column } from 'primeng/primeng';
import { KeyspaceService } from '../api/keyspace.service';
import { DialogContent } from '../shared/Dialog/dialogContent'
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button'
import { PolymerElement } from '@vaadin/angular2-polymer';
import { Observable } from 'rxjs/Observable';
import { ShardService } from '../api/shard.service';
import { DialogComponent } from '../shared/Dialog/dialog.component';
import { DialogSettings } from '../shared/Dialog/dialogSettings';
import { AddButtonComponent } from '../shared/addButton/addButton.component';

@Component({
  moduleId: module.id,
  selector: 'vt-shard-view',
  templateUrl: './shard.component.html',
  styleUrls: ['../styles/vt.style.css'],
  directives: [
              ROUTER_DIRECTIVES,
              MD_CARD_DIRECTIVES,
              MD_BUTTON_DIRECTIVES,
              DataTable,
              Column,
              PolymerElement('paper-dialog'),
              AddButtonComponent,
              DialogComponent],
  providers: [
              KeyspaceService,
              ShardService,
              TabletService],
})
export class ShardComponent implements OnInit, OnDestroy{
  private routeSub: any;
  keyspaceName: string;
  shardName: string;
  keyspace = {};
  tablets = [];
  tabletsReady = false;
  dialogSettings : DialogSettings;
  dialogContent: DialogContent;
  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private keyspaceService: KeyspaceService,
    private shardService: ShardService,
    private tabletService: TabletService) {}
  ngOnInit() {
    this.routeSub = this.router.routerState.queryParams
      .subscribe(params => {
        this.keyspaceName = params['keyspace'];
        this.shardName = params['shard'];
        //this.getKeyspace(this.keyspaceName);
        //this.getTablets(this.keyspaceName, this.shardName);
        
      }
    );
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
  }

  getTablets(keyspaceName, shardName) {
    this.tabletService.getTablets(keyspaceName, shardName).subscribe((tablets) => {
      this.tablets = tablets;
      this.tabletsReady = true;
    });
  }

  getKeyspace(keyspaceName) {
    this.keyspaceService.getKeyspace(keyspaceName).subscribe((keyspaceStreamStream) => {
      keyspaceStreamStream.subscribe( keyspaceStream => {
        keyspaceStream.subscribe ( (keyspace) => {
          this.keyspace = keyspace;
        })
      });
    });
  }

  deleteShard() {
    this.dialogSettings.startPending();
    this.shardService.deleteShard(this.dialogContent).subscribe( resp => {
      if (resp.Error == true) {
        this.dialogSettings.setMessage("There was a problem deleting ", ": " + resp.Output);
        this.dialogSettings.endPending();
      } else {
        this.getKeyspace(this.keyspaceName);
        this.dialogSettings.endPending();
      }
    });
  }

  toggleModal() {
    this.dialogSettings.openModal = !this.dialogSettings.openModal;
  }

  prepareDelete(shardName) {
    this.dialogSettings = new DialogSettings("Delete", this.deleteShard.bind(this), "Delete " + shardName, "Are you sure you want to delete " + shardName + "?");
    let flags = {};
    // TODO(dsslater): Add Flags
    this.dialogContent = new DialogContent(shardName, flags);
  }

  blockClicks(event){
    event.stopPropagation();
  }
}
