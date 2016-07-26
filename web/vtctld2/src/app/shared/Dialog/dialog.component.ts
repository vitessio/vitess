import { Component, OnInit, Input, Output, EventEmitter} from '@angular/core';
import { ROUTER_DIRECTIVES, Router } from '@angular/router';
import { KeyspaceService } from '../../api/keyspace.service';
import { KeyspaceNameFlag, ShardingColumnNameFlag, ShardingColumnTypeFlag, ForceFlag, RecursiveFlag } from '../../shared/flags/keyspace.flags';
import { DialogContent } from '../Dialog/dialogContent'
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_CHECKBOX_DIRECTIVES } from '@angular2-material/checkbox';
import { MD_INPUT_DIRECTIVES } from '@angular2-material/input';
import { MD_PROGRESS_BAR_DIRECTIVES } from '@angular2-material/progress-bar';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button'
import { PolymerElement } from '@vaadin/angular2-polymer';
import { Observable } from 'rxjs/Observable';
import { TabletService } from '../../api/tablet.service'
import { DialogSettings } from '../Dialog/dialogSettings';

@Component({
  moduleId: module.id,
  selector: 'vt-dialog',
  templateUrl: './dialog.component.html',
  styleUrls: ['./dialog.component.css', '../../styles/vt.style.css'],
  providers: [
              KeyspaceService,
              TabletService],
  directives: [
              ROUTER_DIRECTIVES,
              MD_CARD_DIRECTIVES,
              MD_PROGRESS_BAR_DIRECTIVES,
              MD_CHECKBOX_DIRECTIVES,
              MD_INPUT_DIRECTIVES,
              MD_BUTTON_DIRECTIVES,
              PolymerElement('paper-item'),
              PolymerElement('paper-listbox'),
              PolymerElement('paper-dropdown-menu')],
  
})
export class DialogComponent implements OnInit{
  title = 'Vitess Control Panel';
  keyspaces = [];

  @Input() test: string;
  @Input() dialogContent: DialogContent;
  @Input() dialogSettings: DialogSettings;
  @Output() close = new EventEmitter();
  ngOnInit() {
  }

  typeSelected(paramName, e) {
    // Polymer event syntax, waiting on Material2 implementation of dropdown.
    this.dialogContent.setParam(paramName, e.detail.item.__dom.firstChild.data);
  }

  toggleModal() {
    this.dialogSettings.openModal = !this.dialogSettings.openModal;
  }

  closeDialog(){
    this.close.emit({});
  }
  
  sendAction(){
    let resp = this.dialogContent.prepare();
    if (resp.success) {
      this.dialogSettings.actionFunction();
    } else {
      this.dialogSettings.setMessage("There was a problem preparing ", ": " + resp.message);
    }
    this.dialogSettings.dialogForm = false;
    this.dialogSettings.dialogLog = true;
  }
}
