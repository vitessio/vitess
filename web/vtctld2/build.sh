#!/bin/bash
set -e
npm install -g angular-cli
npm install -g bower
npm install
bower install
ng build -prod
