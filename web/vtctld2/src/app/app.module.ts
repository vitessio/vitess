import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { HTTP_PROVIDERS } from '@angular/http';

import { AppComponent }  from './app.component';
import { DashboardComponent } from './dashboard/dashboard.component';
import { KeyspaceComponent } from './dashboard/keyspace.component';
import { SchemaComponent } from './schema/schema.component';
import { ShardComponent } from './dashboard/shard.component';
import { StatusComponent } from './status/status.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';
import { TasksComponent } from './tasks/tasks.component';
import { APP_ROUTER_PROVIDERS, routing } from './app.routes';

import { FormsModule } from '@angular/forms';
import { MdButtonModule } from '@angular2-material/button';
import { MdRippleModule } from '@angular2-material/core/ripple/ripple';

@NgModule({
  imports:      [ BrowserModule, routing, FormsModule, MdButtonModule, MdRippleModule],
  declarations: [
    AppComponent,
    DashboardComponent,
    KeyspaceComponent,
    SchemaComponent,
    ShardComponent,
    StatusComponent,
    TopoBrowserComponent,
    TasksComponent,
  ],
  providers:    [ APP_ROUTER_PROVIDERS, HTTP_PROVIDERS ],
  bootstrap:    [ AppComponent ]
})
export class AppModule { }
