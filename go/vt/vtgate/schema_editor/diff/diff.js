/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function DiffController($scope, curSchema) {
  init();

  function init() {
    $scope.keyspaces = curSchema.keyspaces;
    $scope.keyspacesJSON = angular.toJson($scope.keyspaces, true);
    $scope.original = curSchema.original;
    $scope.originalJSON = angular.toJson($scope.original, true);
  }
}