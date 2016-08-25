import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { NgModule, CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';

import { MdButtonModule } from '@angular2-material/button/button';
import { MdCardModule } from '@angular2-material/card/card';
import { MdCheckboxModule } from '@angular2-material/checkbox/checkbox';
import { MdIconModule } from '@angular2-material/icon/icon';
import { MdInputModule } from '@angular2-material/input/input';
import { MdListModule } from '@angular2-material/list/list';
import { MdProgressBarModule } from '@angular2-material/progress-bar/progress-bar';
import { MdRippleModule } from '@angular2-material/core/ripple/ripple';
import { MdSidenavModule } from '@angular2-material/sidenav/sidenav';
import { MdToolbarModule } from '@angular2-material/toolbar/toolbar';

import { DataTableModule, DialogModule, DropdownModule, SharedModule } from 'primeng/primeng';

import { AddButtonComponent } from './shared/add-button.component';
import { APP_ROUTER_PROVIDERS, routing } from './app.routes';
import { AppComponent } from './app.component';
import { BreadcrumbsComponent } from './shared/breadcrumbs.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { DialogComponent } from './shared/dialog/dialog.component';
import { HeatmapComponent } from './status/heatmap.component';
import { KeyspaceComponent } from './dashboard/keyspace.component';
import { SchemaComponent } from './schema/schema.component';
import { ShardComponent } from './dashboard/shard.component';
import { StatusComponent } from './status/status.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';
import { TabletComponent } from './dashboard/tablet.component';
import { TasksComponent } from './tasks/tasks.component';

import { KeyspaceService } from './api/keyspace.service';
import { ShardService } from './api/shard.service';
import { TabletService } from './api/tablet.service';
import { TabletStatusService } from './api/tablet-status.service';
import { TopoDataService } from './api/topo-data.service';
import { VtctlService } from './api/vtctl.service';

@NgModule({
  imports: [
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
    MdSidenavModule,
    MdToolbarModule,
    routing,
    SharedModule,
  ],
  declarations: [
    AddButtonComponent,
    AppComponent,
    BreadcrumbsComponent,
    DashboardComponent,
    DialogComponent,
    HeatmapComponent,
    KeyspaceComponent,
    SchemaComponent,
    ShardComponent,
    StatusComponent,
    TopoBrowserComponent,
    TabletComponent,
    TasksComponent,
  ],
  providers: [
    APP_ROUTER_PROVIDERS,
    KeyspaceService,
    ShardService,
    TabletService,
    TabletStatusService,
    TopoDataService,
    VtctlService,
  ],
  entryComponents: [AppComponent],
  bootstrap: [AppComponent],
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
})
export class AppModule { }
