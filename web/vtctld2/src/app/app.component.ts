import { Component } from '@angular/core';
import { MD_TOOLBAR_DIRECTIVES } from '@angular2-material/toolbar';
import { MD_SIDENAV_DIRECTIVES } from '@angular2-material/sidenav';
import { MD_LIST_DIRECTIVES } from '@angular2-material/list';
import { MdIcon, MdIconRegistry } from '@angular2-material/icon';
import { ROUTER_DIRECTIVES } from '@angular/router';
import { WorkqueueComponent } from './workqueue/workqueue.component'

import './rxjs-operators';

@Component({
  moduleId: module.id,
  selector: 'app-root',
  templateUrl: 'app.component.html',
  styleUrls: ['app.component.css'],
  providers: [
    MdIconRegistry
  ],
  directives: [
    MD_TOOLBAR_DIRECTIVES,
    MD_SIDENAV_DIRECTIVES,
    MD_LIST_DIRECTIVES,
    MdIcon,
    ROUTER_DIRECTIVES,
    WorkqueueComponent,
  ],
})
export class AppComponent {
  title = 'Vitess Control Panel';
}
