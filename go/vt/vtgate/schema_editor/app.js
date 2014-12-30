/**
 * Copyright 2014, Google Inc. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
'use strict';

angular.module('app', ['ngRoute'])
.factory('curSchema', curSchema)
.controller('SchemaController', ['$scope', '$routeParams', 'curSchema', SchemaController])
.controller('ClassController', ClassController)
.controller('VindexController', VindexController)
.controller('DiffController', DiffController)
.config(['$routeProvider', function($routeProvider) {
  $routeProvider
  .when('/',{
    templateUrl: "editor/editor.html",
    controller: "SchemaController"
  })
  .when('/editor/:keyspaceName',{
    templateUrl: "editor/editor.html",
    controller: "SchemaController"
  })
  .when('/editor/:keyspaceName/class/:className',{
    templateUrl: "editor/class/class.html",
    controller: "ClassController"
  })
  .when('/editor/:keyspaceName/vindex/:vindexName',{
    templateUrl: "editor/vindex/vindex.html",
    controller: "VindexController"
  })
  .when('/diff',{
    templateUrl: "diff/diff.html",
    controller: "DiffController"
  })
  .otherwise({redirectTo: '/'});
}]);