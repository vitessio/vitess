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
.controller('DiffController', DiffController)
.config(['$routeProvider', function($routeProvider) {
  $routeProvider
  .when('/',{
    templateUrl: "editor/keyspace.html",
    controller: "KeyspaceController"
  })
  .when('/editor/:keyspaceName',{
    templateUrl: "editor/keyspace.html",
    controller: "KeyspaceController"
  })
  .when('/editor/:keyspaceName/class/:className',{
    templateUrl: "editor/class/class.html",
    controller: "ClassController"
  })
  .when('/diff',{
    templateUrl: "diff/diff.html",
    controller: "DiffController"
  })
  .otherwise({redirectTo: '/'});
}]);