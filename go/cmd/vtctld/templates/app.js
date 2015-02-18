/**
 * Copyright 2014, Google Inc. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
'use strict';

angular.module('app', ['ngRoute'])
.factory('vindexInfo', vindexInfo)
.factory('curSchema', curSchema)
.controller('KeyspaceController', KeyspaceController)
.controller('SidebarController', SidebarController)
.controller('ClassController', ClassController)
.controller('LoadController', LoadController)
.controller('SubmitController', SubmitController)
.config(['$routeProvider', function($routeProvider) {
  $routeProvider
  .when('/editor',{
    templateUrl: "/content/editor/keyspace.html",
    controller: "KeyspaceController"
  })
  .when('/editor/:keyspaceName',{
    templateUrl: "/content/editor/keyspace.html",
    controller: "KeyspaceController"
  })
  .when('/editor/:keyspaceName/class/:className',{
    templateUrl: "/content/editor/class/class.html",
    controller: "ClassController"
  })
  .when('/load',{
    templateUrl: "/content/load/load.html",
    controller: "LoadController"
  })
  .when('/submit',{
    templateUrl: "/content/submit/submit.html",
    controller: "SubmitController"
  })
  .otherwise({redirectTo: '/'});
}]);