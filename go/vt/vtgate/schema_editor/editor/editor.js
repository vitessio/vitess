/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function SchemaController($scope, $routeParams, curSchema) {
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
    if (!$routeParams.keyspace) {
      return;
    }
    $scope.curKeyspace = $routeParams.keyspace;
    var ks = $scope.keyspaces[$scope.curKeyspace];
    if (ks['Sharded']) {
      $scope.validVindexes = Object.keys(ks.Vindexes);
    }
  }

  $scope.setVindex = function($colVindex, $vindex) {
    $colVindex.Name = $vindex;
  };

  $scope.setVindexType = function($vindex, $vindexType) {
    $vindex.Type = $vindexType;
  };

  $scope.isVindexValid = function($vindexName) {
    return $vindexName in $scope.keyspaces[$scope.curKeyspace].Vindexes;
  };
}