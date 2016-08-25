import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { NgModule, CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';

import { MdButtonModule } from '@angular2-material/button';
import { MdCardModule } from '@angular2-material/card';
import { MdCheckboxModule } from '@angular2-material/checkbox';
import { MdIconModule } from '@angular2-material/icon';
import { MdInputModule } from '@angular2-material/input';
import { MdListModule } from '@angular2-material/list/list';
import { MdProgressBarModule } from '@angular2-material/progress-bar';
import { MdRippleModule } from '@angular2-material/core/ripple/ripple';

import { AccordionModule, DataTableModule, DialogModule, DropdownModule, SharedModule } from 'primeng/primeng';

import { AppComponent } from './app.component';
import { APP_ROUTER_PROVIDERS, routing } from './app.routes';
import { DashboardComponent } from './dashboard/dashboard.component';
import { KeyspaceComponent } from './dashboard/keyspace.component';
import { SchemaComponent } from './schema/schema.component';
import { ShardComponent } from './dashboard/shard.component';
import { StatusComponent } from './status/status.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';
import { TabletComponent } from './dashboard/tablet.component';
import { TasksComponent } from './tasks/tasks.component';

@NgModule({
  imports: [
    AccordionModule,
    BrowserModule,
    DataTableModule,
    DialogModule,
    DropdownModule,
    FormsModule,
    HttpModule,
    MdButtonModule,
    MdCardModule,
    MdCheckboxModule,
    MdIconModule,
    MdInputModule,
    MdListModule,
    MdProgressBarModule,
    MdRippleModule,
    routing,
    SharedModule,
  ],
  declarations: [
    AppComponent,
    DashboardComponent,
    KeyspaceComponent,
    SchemaComponent,
    ShardComponent,
    StatusComponent,
    TopoBrowserComponent,
    TabletComponent,
    TasksComponent,
  ],
  providers: [APP_ROUTER_PROVIDERS],
  entryComponents: [AppComponent],
  bootstrap: [AppComponent],
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
})
export class AppModule { }
