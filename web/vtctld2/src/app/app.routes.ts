import { provideRouter, RouterConfig } from '@angular/router';

import { DashboardComponent } from './dashboard/dashboard.component';
import { SchemaComponent } from './schema/schema.component';
import { StatusComponent } from './status/status.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';
import { WorkqueueComponent } from './workqueue/workqueue.component'
import { KeyspaceComponent } from './keyspace/keyspace.component'



export const routes: RouterConfig = [
	{ path: '', component: DashboardComponent},
	{ path: 'dashboard', component: DashboardComponent},
	{ path: 'status', component: StatusComponent},
	{ path: 'schema', component: SchemaComponent},
	{ path: 'topo', component: TopoBrowserComponent },
        { path: 'queue', component: WorkqueueComponent},
        { path: 'keyspace/:keyspaceName', component: KeyspaceComponent},

];

export const APP_ROUTER_PROVIDERS = [
  provideRouter(routes)
];
