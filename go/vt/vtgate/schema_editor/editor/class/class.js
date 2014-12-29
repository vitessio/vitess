/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function ClassController($scope, $routeParams, curSchema) {
  init();

  function init() {
    $scope.keyspaces = curSchema.keyspaces;
    $scope.vindexTypes = [
        "numeric",
        "hash",
        "hash_autoinc",
        "lookup_hash",
        "lookup_hash_unique",
        "lookup_hash_autoinc",
        "lookup_hash_unique_autoinc"
    ];
    if (!$routeParams.keyspaceName || !$scope.keyspaces[$routeParams.keyspaceName]) {
      return;
    }
    $scope.keyspaceName = $routeParams.keyspaceName;
    $scope.keyspace = $scope.keyspaces[$routeParams.keyspaceName];
    $scope.vindexNames = Object.keys($scope.keyspace.Vindexes);
    if (!$routeParams.className || !$scope.keyspace.Classes[$routeParams.className]) {
      return;
    }
    $scope.className = $routeParams.className;
    $scope.klass = $scope.keyspace.Classes[$routeParams.className];
  }

  $scope.setVindex = function($colVindex, $vindex) {
    $colVindex.Name = $vindex;
  };

  $scope.addVindex = function($colName, $vindex) {
    $scope.klass.push({
      "Col": $colName,
      "Name": $vindex
    });
  };

  $scope.setVindexType = function($vindex, $vindexType) {
    $vindex.Type = $vindexType;
  };

  $scope.isClassValid = function($className) {
    return $className in $scope.keyspace.Classes;
  };

  $scope.isVindexValid = function($vindexName) {
    return $vindexName in $scope.keyspace.Vindexes;
  };
}