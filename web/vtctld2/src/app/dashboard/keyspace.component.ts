import { ActivatedRoute, Router, ROUTER_DIRECTIVES } from '@angular/router';
import { Component, OnInit, OnDestroy } from '@angular/core';

import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';

import { Observable } from 'rxjs/Observable';

import { AddButtonComponent } from '../shared/add-button.component';
import { DialogComponent } from '../shared/dialog/dialog.component';
import { DialogContent } from '../shared/dialog/dialog-content';
import { DialogSettings } from '../shared/dialog/dialog-settings';
import { KeyspaceNameFlag, LowerBoundFlag, ShardNameFlag, UpperBoundFlag } from '../shared/flags/shard.flags';
import { KeyspaceExtraComponent } from './keyspace-extra.component';
import { KeyspaceService } from '../api/keyspace.service';
import { PrepareResponse } from '../shared/prepare-response';
import { ShardService } from '../api/shard.service';

@Component({
  selector: 'vt-keyspace-view',
  templateUrl: './keyspace.component.html',
  styleUrls: ['../styles/vt.style.css'],
  providers: [
    KeyspaceService,
    ShardService
  ],
  directives: [
    ROUTER_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    MD_BUTTON_DIRECTIVES,
    DialogComponent,
    AddButtonComponent,
  ],
})
export class KeyspaceComponent implements OnInit, OnDestroy {
  private routeSub: any;
  keyspaceName: string;
  shardsReady = false;
  keyspace = {};
  dialogSettings: DialogSettings;
  dialogContent: DialogContent;
  keyspaceExtraComponent = KeyspaceExtraComponent;
  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private keyspaceService: KeyspaceService,
    private shardService: ShardService) {}

  ngOnInit() {
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();
    this.routeSub = this.router.routerState.queryParams
      .subscribe(params => {
        this.keyspaceName = params['keyspace'];
        this.getKeyspace(this.keyspaceName);
      }
    );
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
  }

  getKeyspace(keyspaceName) {
    this.keyspaceService.getKeyspace(keyspaceName).subscribe(keyspaceStream => {
      keyspaceStream.subscribe(keyspace => {
          this.keyspace = keyspace;
      });
    });
  }

  createShard() {
    this.dialogSettings.startPending();
    this.shardService.createShard(this.dialogContent).subscribe(resp => {
      if (resp.Error) {
        this.dialogSettings.setMessage('There was a problem creating ', `: ${resp.Output}`);
      } else {
        this.getKeyspace(this.keyspaceName);
      }
      this.dialogSettings.endPending();
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

  prepareNew() {
    this.dialogSettings = new DialogSettings('Create', this.createShard.bind(this), 'Create a new Shard', '');
    this.dialogSettings.setMessage('Created', '');
    let flags = {};
    flags['keyspaceName'] = new KeyspaceNameFlag(0, 'keyspaceName', this.keyspaceName);
    flags['shardName'] = new ShardNameFlag(1, 'shardName', '');
    flags['lowerBound'] = new LowerBoundFlag(2, 'lowerBound', '');
    flags['upperBound'] = new UpperBoundFlag(3, 'upperBound', '');
    this.dialogContent = new DialogContent('', flags);
  }

  prepareDelete(shardName) {
    this.dialogSettings = new DialogSettings('Delete', this.deleteShard.bind(this),
                                             'Delete ' + shardName, `Are you sure you want to delete ${shardName}?`);
    this.dialogSettings.setMessage('Deleted', '');
    let flags = {};
    // TODO(dsslater): Add Flags
    this.dialogContent = new DialogContent(shardName, flags);
  }

  blockClicks(event) {
    event.stopPropagation();
  }

  prepare(flags) {
    flags['shardName'].setValue(this.getName(flags['lowerBound'].getStrValue(), flags['upperBound'].getStrValue()));
    return new PrepareResponse(true, flags);
  }

  navigate(keyspaceName, shardName) {
    this.router.navigate(['/shard'], {queryParams: { keyspace: keyspaceName, shard: shardName}});
  }

  // Functions for parsing shardName
  getName(lowerBound, upperBound) {
    this.dialogContent.name = lowerBound + '-' + upperBound;
    return this.dialogContent.name;
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
