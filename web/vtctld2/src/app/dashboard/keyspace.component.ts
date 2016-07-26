import { Component, Input, OnInit, OnDestroy } from '@angular/core';
import { ROUTER_DIRECTIVES, ActivatedRoute, Router } from '@angular/router';
import { KeyspaceNameFlag, ShardNameFlag, LowerBoundFlag, UpperBoundFlag, ForceFlag, RecursiveFlag} from '../shared/flags/shard.flags'
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
  selector: 'vt-keyspace-view',
  templateUrl: './keyspace.component.html',
  styleUrls: ['../styles/vt.style.css'],
  providers: [KeyspaceService,
              ShardService],
  directives: [
              ROUTER_DIRECTIVES,
              MD_CARD_DIRECTIVES,
              MD_BUTTON_DIRECTIVES,
              PolymerElement('paper-dialog'),
              DialogComponent,
              AddButtonComponent],
})
export class KeyspaceComponent implements OnInit, OnDestroy{
  private routeSub: any;
  keyspaceName: string;
  shardsReady = false;
  keyspace = {};
  dialogSettings : DialogSettings;
  dialogContent: DialogContent;
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
    this.keyspaceService.getKeyspace(keyspaceName).subscribe((keyspaceStreamStream) => {
      keyspaceStreamStream.subscribe( keyspaceStream => {
        keyspaceStream.subscribe ( (keyspace) => {
          this.keyspace = keyspace;
        })
      });
    });
  }

  createShard() {
    this.dialogSettings.startPending();
    this.shardService.createShard(this.dialogContent).subscribe( resp => {
      if (resp.Error == true) {
        this.dialogSettings.setMessage("There was a problem creating ", ": " + resp.Output);
        this.dialogSettings.endPending();
      } else {
        this.getKeyspace(this.keyspaceName);
        this.dialogSettings.endPending();
      }
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

  prepareNew(){
    this.dialogSettings = new DialogSettings("Create", this.createShard.bind(this), "Create a new Shard", "");
    let flags = {};
    flags["keyspaceName"] = new KeyspaceNameFlag(0, "keyspaceName", this.keyspaceName);
    flags["shardName"] = new ShardNameFlag(1, "shardName", "");
    flags["lowerBound"] = new LowerBoundFlag(2, "lowerBound", "");
    flags["upperBound"] = new UpperBoundFlag(3, "upperBound", "");
    this.dialogContent = new DialogContent("", flags);
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

  navigate(keyspaceName, shardName) {
    this.router.navigate(["/shard"], {queryParams: { keyspace: keyspaceName, shard: shardName}});
  }

  // Functions for parsing shardName
  getName(lowerBound, upperBound) {
    let LB = lowerBound == "0" ? "": lowerBound;
    let UB = upperBound == "0" ? "": upperBound;
    let full = LB + "-" + UB;
    this.dialogContent.name = full == "-" ? "0": full;
    return this.dialogContent.name;
  }

  getLowerBound(shard) {
    let index = shard.indexOf('-');
    if (index <= 0) {
      return "";
    } else {
      return shard.substring(0, index);
    }
  }

  getUpperBound(shard){
    let index = shard.indexOf('-');
    return shard.substring(index + 1);
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
