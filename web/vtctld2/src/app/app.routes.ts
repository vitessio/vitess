import { provideRouter, RouterConfig } from '@angular/router';
import { DashboardComponent } from './dashboard/dashboard.component';
import { StatusComponent } from './status/status.component';
import { SchemaComponent } from './schema/schema.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';
import { WorkqueueComponent } from './workqueue/workqueue.component';
import { KeyspaceViewComponent } from './dashboard/keyspaceView/keyspaceView.component';
import { ShardViewComponent } from './dashboard/shardView/shardView.component';


export const routes: RouterConfig = [
	{ path: '', component: DashboardComponent},
	{ path: 'dashboard', component: DashboardComponent},
	{ path: 'status', component: StatusComponent},
	{ path: 'schema', component: SchemaComponent},
	{ path: 'topo', component: TopoBrowserComponent },
  { path: 'queue', component: WorkqueueComponent},
  { path: 'keyspace/:keyspaceName', component: KeyspaceViewComponent},
  { path: 'keyspace/:keyspaceName/cell/:shardName', component: ShardViewComponent},

];

export const APP_ROUTER_PROVIDERS = [
  provideRouter(routes)
];