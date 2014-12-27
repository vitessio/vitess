/**
 * Copyright 2014, Google Inc. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
'use strict';

function SchemaController($scope, curSchema) {
  init();

  $scope.setKeyspace = function($curKeyspace) {
    $scope.curKeyspace = $curKeyspace;
    var ks = $scope.vschema.Keyspaces[$curKeyspace];
    if (ks.Sharded) {
      $scope.validVindexes = Object.keys(ks.Vindexes);
    }
  };

  $scope.setVindex = function($colVindex, $vindex) {
    $colVindex.Name = $vindex;
  };

  $scope.setVindexType = function($vindex, $vindexType) {
    $vindex.Type = $vindexType;
  };

  $scope.indexValid = function($vindexName) {
    return $vindexName in $scope.vschema.Keyspaces[$scope.curKeyspace].Vindexes;
  };

  function init() {
    $scope.vschema = curSchema.vschema;
    $scope.curKeyspace = "";
    $scope.vindexTypes = [
        "numeric",
        "hash",
        "hash_autoinc",
        "lookup_hash",
        "lookup_hash_unique",
        "lookup_hash_autoinc",
        "lookup_hash_unique_autoinc"
    ];
  };
}