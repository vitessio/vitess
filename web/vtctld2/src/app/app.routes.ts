import { Routes, RouterModule} from '@angular/router';

import { CanDeactivateGuard } from './shared/can-deactivate-guard';
import { DashboardComponent } from './dashboard/dashboard.component';
import { KeyspaceComponent } from './dashboard/keyspace.component';
import { SchemaComponent } from './schema/schema.component';
import { ShardComponent } from './dashboard/shard.component';
import { StatusComponent } from './status/status.component';
import { TabletComponent } from './dashboard/tablet.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';
import { WorkflowListComponent } from './workflows/workflow-list.component';

export const routes: Routes = [
  { path: '', component: DashboardComponent},
  { path: 'dashboard', component: DashboardComponent, canDeactivate: [CanDeactivateGuard]},
  { path: 'status', component: StatusComponent},
  { path: 'schema', component: SchemaComponent},
  { path: 'tablet', component: TabletComponent},
  { path: 'workflows', component: WorkflowListComponent},
  { path: 'topo', component: TopoBrowserComponent },
  { path: 'keyspace', component: KeyspaceComponent},
  { path: 'shard', component: ShardComponent},
];

export const routing = RouterModule.forRoot(routes);

export const APP_ROUTER_PROVIDERS = [
  CanDeactivateGuard
];
