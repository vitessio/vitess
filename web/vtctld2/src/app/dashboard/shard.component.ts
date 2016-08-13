import { ActivatedRoute, Router, ROUTER_DIRECTIVES } from '@angular/router';
import { Component, OnDestroy, OnInit } from '@angular/core';

import { Column, DataTable } from 'primeng/primeng';

import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';

import { Observable } from 'rxjs/Observable';

import { AddButtonComponent } from '../shared/add-button.component';
import { DialogComponent } from '../shared/dialog/dialog.component';
import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { KeyspaceService } from '../api/keyspace.service';
import { ShardService } from '../api/shard.service';
import { TabletService } from '../api/tablet.service';

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
    AddButtonComponent,
    DialogComponent
  ],
  providers: [
    KeyspaceService,
    ShardService,
    TabletService
  ],
})
export class ShardComponent implements OnInit, OnDestroy {
  private routeSub: any;
  keyspaceName: string;
  shardName: string;
  keyspace = {};
  tablets = [];
  tabletsReady = false;
  dialogSettings: DialogSettings;
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
        // this.getKeyspace(this.keyspaceName);
        // this.getTablets(this.keyspaceName, this.shardName);
      }
    );
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
  }

  getTablets(keyspaceName, shardName) {
    this.tabletService.getTablets(keyspaceName, shardName).subscribe(tablets => {
      this.tablets = tablets;
      this.tabletsReady = true;
    });
  }

  getKeyspace(keyspaceName) {
    this.keyspaceService.getKeyspace(keyspaceName).subscribe(keyspaceStreamStream => {
      keyspaceStreamStream.subscribe(keyspaceStream => {
        keyspaceStream.subscribe(keyspace => {
          this.keyspace = keyspace;
        });
      });
    });
  }

  deleteShard() {
    this.dialogSettings.startPending();
    this.shardService.deleteShard(this.dialogContent).subscribe(resp => {
      if (resp.Error) {
        this.dialogSettings.setMessage('There was a problem deleting ', `: ${resp.Output}`);
      } else {
        this.getKeyspace(this.keyspaceName);
      }
      this.dialogSettings.endPending();
    });
  }

  toggleModal() {
    this.dialogSettings.openModal = !this.dialogSettings.openModal;
  }

  prepareDelete(shardName) {
    this.dialogSettings = new DialogSettings('Delete', this.deleteShard.bind(this),
                                             `Delete ${shardName}`, `Are you sure you want to delete ${shardName}?`);
    this.dialogSettings.setMessage('Deleted', '');
    let flags = {};
    // TODO(dsslater): Add Flags
    this.dialogContent = new DialogContent(shardName, flags);
  }

  blockClicks(event) {
    event.stopPropagation();
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
