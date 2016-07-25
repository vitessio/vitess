import { provideRouter, RouterConfig } from '@angular/router';
import { DashboardComponent } from './dashboard/dashboard.component';
import { StatusComponent } from './status/status.component';
import { SchemaComponent } from './schema/schema.component';
import { TopoBrowserComponent } from './topo/topo-browser.component';
import { TabletStatusComponent } from './status/tablet.component';
import { PlotlyComponent } from './status/heatmap.component';
import { ChartComponent } from './status/chartHeatmap.component';

export const routes: RouterConfig = [
  { path: '', component: DashboardComponent },
  { path: 'dashboard', component: DashboardComponent },
  { path: 'status', component: StatusComponent },
  { path: 'schema', component: SchemaComponent },
  { path: 'topo', component: TopoBrowserComponent },

  { path: 'tabletStatus', component: TabletStatusComponent },
  { path: 'heatmap', component: PlotlyComponent },
  { path: 'heatmap2', component: ChartComponent },
];

export const APP_ROUTER_PROVIDERS = [
  provideRouter(routes)
];
