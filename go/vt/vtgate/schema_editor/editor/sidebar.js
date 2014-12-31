/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function SidebarController($scope, $routeParams, curSchema) {
  $scope.addKeyspace = function($keyspaceName, $sharded) {
    AddKeyspace(curSchema.keyspaces, $keyspaceName, $sharded)
  }
  $scope.reset = function() {
    curSchema.reset();
  };
}