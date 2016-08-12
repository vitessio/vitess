import { NgModule }      from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppComponent }  from './app.component';
import { APP_ROUTER_PROVIDERS, routing } from './app.routes';
import { HTTP_PROVIDERS } from '@angular/http';

import { FormsModule } from '@angular/forms';
import { MdButtonModule } from '@angular2-material/button';
import { MdRippleModule } from '@angular2-material/core/ripple/ripple';

@NgModule({
  imports:      [ BrowserModule, routing, FormsModule, MdButtonModule, MdRippleModule],
  declarations: [ AppComponent ],
  providers:    [ APP_ROUTER_PROVIDERS, HTTP_PROVIDERS ],
  bootstrap:    [ AppComponent ]
})
export class AppModule { }
