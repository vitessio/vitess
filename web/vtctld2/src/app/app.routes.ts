import { provideRouter, RouterConfig } from '@angular/router';

//import { CrisisListComponent }  from './crisis-list.component';
//import { HeroListComponent }    from './hero-list.component';

export const routes: RouterConfig = [
  //  { path: 'crisis-center', component: CrisisListComponent },
  //  { path: 'heroes', component: HeroListComponent }
];

export const APP_ROUTER_PROVIDERS = [
  provideRouter(routes)
];