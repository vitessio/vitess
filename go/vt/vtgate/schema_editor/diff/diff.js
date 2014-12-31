/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function DiffController($scope, curSchema) {
  init();

  function init() {
    $scope.keyspacesJSON = angular.toJson(curSchema.keyspaces, true);
    $scope.originalJSON = angular.toJson(curSchema.original, true);
  }
}