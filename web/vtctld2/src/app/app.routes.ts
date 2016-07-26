import { provideRouter, RouterConfig } from '@angular/router';

import { DashboardComponent } from './dashboard/dashboard.component';
import { SchemaComponent } from './schema/schema.component';
import { StatusComponent } from './status/status.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';
import { WorkqueueComponent } from './workqueue/workqueue.component';
import { KeyspaceComponent } from './dashboard/keyspace.component';
import { ShardComponent } from './dashboard/shard.component';
import { CanDeactivateGuard } from './shared/canDeactivateGuard/canDeactivateGuard'

export const routes: RouterConfig = [
  { path: '', component: DashboardComponent},
  { path: 'dashboard', component: DashboardComponent, canDeactivate: [CanDeactivateGuard]},
  { path: 'status', component: StatusComponent},
  { path: 'schema', component: SchemaComponent},
  { path: 'topo', component: TopoBrowserComponent },
  { path: 'queue', component: WorkqueueComponent},
  { path: 'keyspace', component: KeyspaceComponent},
  { path: 'shard', component: ShardComponent},

];

export const APP_ROUTER_PROVIDERS = [
  provideRouter(routes),
  CanDeactivateGuard
];