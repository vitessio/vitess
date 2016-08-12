import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent }  from './app.component';
import { APP_ROUTER_PROVIDERS } from './app.routes';
import { HTTP_PROVIDERS } from '@angular/http';


@NgModule({
  imports:      [ BrowserModule ],
  declarations: [ AppComponent ],
  providers:    [ APP_ROUTER_PROVIDERS, HTTP_PROVIDERS ],
  bootstrap:    [ AppComponent ]
})
export class AppModule { }
