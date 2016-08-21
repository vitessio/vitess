import { BrowserModule } from '@angular/platform-browser';
import { NgModule, CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';

import { AppComponent } from './app.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { KeyspaceComponent } from './dashboard/keyspace.component';
import { SchemaComponent } from './schema/schema.component';
import { ShardComponent } from './dashboard/shard.component';
import { StatusComponent } from './status/status.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';
import { TabletComponent } from './dashboard/tablet.component';
import { TasksComponent } from './tasks/tasks.component';
import { APP_ROUTER_PROVIDERS, routing } from './app.routes';

import { MdButtonModule } from '@angular2-material/button';
import { MdRippleModule } from '@angular2-material/core/ripple/ripple';
import { MdCardModule } from '@angular2-material/card';
import { MdCheckboxModule } from '@angular2-material/checkbox';
import { MdInputModule } from '@angular2-material/input';
import { MdListModule } from '@angular2-material/list/list';
import { MdProgressBarModule } from '@angular2-material/progress-bar';

import { PolymerElement } from '@vaadin/angular2-polymer';

import { KeyspaceService } from './api/keyspace.service'


const PolymerComponents = [
  PolymerElement('paper-dialog'),
  PolymerElement('paper-dropdown-menu'),
  PolymerElement('paper-item'),
  PolymerElement('paper-listbox'),
];

@NgModule({
  imports:      [
    BrowserModule,
    routing,
    FormsModule,
    MdButtonModule,
    MdRippleModule,
    MdCardModule,
    MdCheckboxModule,
    MdInputModule,
    MdListModule,
    MdProgressBarModule,
    HttpModule,
  ],
  declarations: [
    AppComponent,
    DashboardComponent,
    KeyspaceComponent,
    PolymerComponents,
    SchemaComponent,
    ShardComponent,
    StatusComponent,
    TabletComponent,
    TasksComponent,
    TopoBrowserComponent,
  ],
  providers: [APP_ROUTER_PROVIDERS, KeyspaceService],
  entryComponents: [AppComponent],
  bootstrap: [AppComponent],
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
})
export class AppModule { }
