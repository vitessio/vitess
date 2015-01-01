/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function ClassController($scope, $routeParams, vindexInfo, curSchema) {
  init();

  function init() {
    $scope.curSchema = curSchema;
    $scope.vindexInfo = vindexInfo;
    if (!$routeParams.keyspaceName || !curSchema.keyspaces[$routeParams.keyspaceName]) {
      return;
    }
    $scope.keyspaceName = $routeParams.keyspaceName;
    $scope.keyspace = curSchema.keyspaces[$routeParams.keyspaceName];
    if (!$routeParams.className || !$scope.keyspace.Classes[$routeParams.className]) {
      return;
    }
    $scope.className = $routeParams.className;
    $scope.klass = $scope.keyspace.Classes[$routeParams.className];
    $scope.classEditor = {};
  }

  $scope.setName = function($colVindex, $vindex) {
    $colVindex.Name = $vindex;
    $scope.clearClassError();
  };

  $scope.addColVindex = function($colName, $vindex) {
    if (!$colName) {
      $scope.classEditor.err = "empty column name";
      return;
    }
    for (var i = 0; i < $scope.klass.length; i++) {
      if ($colName == $scope.klass[i].Col) {
        $scope.classEditor.err = $colName + " already exists";
        return;
      }
    }
    ;
    $scope.klass.push({
        "Col": $colName,
        "Name": $vindex
    });
    $scope.clearClassError();
  };

  $scope.deleteColVindex = function(index) {
    $scope.klass.splice(index, 1);
    $scope.clearClassError();
  };

  $scope.clearClassError = function() {
    $scope.classEditor.err = "";
  };

  $scope.validVindexes = function($className, $index) {
    return curSchema.validVindexes($scope.keyspace, $className, $index);
  };

  $scope.vindexHasError = function($className, $index) {
    return curSchema.vindexHasError($scope.keyspace, $className, $index);
  };
}
