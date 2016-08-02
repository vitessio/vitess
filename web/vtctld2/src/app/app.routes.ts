import { provideRouter, RouterConfig } from '@angular/router';

import { DashboardComponent } from './dashboard/dashboard.component';
import { SchemaComponent } from './schema/schema.component';
import { StatusComponent } from './status/status.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';

export const routes: RouterConfig = [
  { path: '', component: DashboardComponent },
  { path: 'dashboard', component: DashboardComponent },
  { path: 'status', component: StatusComponent },
  { path: 'schema', component: SchemaComponent },
  { path: 'topo', component: TopoBrowserComponent },
];

export const APP_ROUTER_PROVIDERS = [
  provideRouter(routes)
];
