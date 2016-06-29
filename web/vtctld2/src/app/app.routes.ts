import { provideRouter, RouterConfig } from '@angular/router';
import { DashboardComponent } from './dashboard/dashboard.component'
import { StatusComponent } from './status/status.component'
import { SchemaComponent } from './schema/schema.component'
import { TopoComponent } from './topo/topo.component'

//import { CrisisListComponent }  from './crisis-list.component';
//import { HeroListComponent }    from './hero-list.component';

export const routes: RouterConfig = [
	{ path: '', component: DashboardComponent},
	{ path: 'dashboard', component: DashboardComponent},
	{ path: 'status', component: StatusComponent},
	{ path: 'schema', component: SchemaComponent},
	{ path: 'topo', component: TopoComponent},
];

export const APP_ROUTER_PROVIDERS = [
  provideRouter(routes)
];