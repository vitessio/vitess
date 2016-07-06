import { provideRouter, RouterConfig } from '@angular/router';

import { CanDeactivateGuard } from './shared/can-deactivate-guard';
import { DashboardComponent } from './dashboard/dashboard.component';
import { KeyspaceComponent } from './dashboard/keyspace.component';
import { SchemaComponent } from './schema/schema.component';
import { ShardComponent } from './dashboard/shard.component';
import { StatusComponent } from './status/status.component';
import { TasksComponent } from './tasks/tasks.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';

export const routes: RouterConfig = [
  { path: '', component: DashboardComponent},
  { path: 'dashboard', component: DashboardComponent, canDeactivate: [CanDeactivateGuard]},
  { path: 'status', component: StatusComponent},
  { path: 'schema', component: SchemaComponent},
  { path: 'tasks', component: TasksComponent},
  { path: 'topo', component: TopoBrowserComponent },
  { path: 'keyspace', component: KeyspaceComponent},
  { path: 'shard', component: ShardComponent},

];

export const APP_ROUTER_PROVIDERS = [
  provideRouter(routes),
  CanDeactivateGuard
];