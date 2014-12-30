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
    if (!$routeParams.keyspaceName || !$scope.keyspaces[$routeParams.keyspaceName]) {
      return;
    }
    $scope.keyspaceName = $routeParams.keyspaceName;
    $scope.keyspace = $scope.keyspaces[$routeParams.keyspaceName];
    if ($scope.keyspace.Sharded) {
      $scope.vindexNames = Object.keys($scope.keyspace.Vindexes);
    }
    $scope.tableEditor = {};
  }
  
  $scope.addTable = function ($tableName, $className) {
    if ($scope.keyspace.Tables[$tableName]) {
      $scope.tableEditor.err = $tableName + " already exists";
      return
    }
    $scope.setTableClass($tableName, $className);
    $scope.tableEditor.newTableName = "";
    $scope.clearTableError();
  };

  $scope.setTableClass = function($tableName, $className) {
    $scope.keyspace.Tables[$tableName] = $className;
    $scope.clearTableError();
  };
  
  $scope.deleteTable = function($tableName) {
    delete $scope.keyspace.Tables[$tableName];
    $scope.clearTableError();
  };
  
  $scope.clearTableError= function() {
    $scope.tableEditor.err = "";
  };
  
  $scope.addClass = function($className) {
    $scope.keyspace.Classes[$className] = {};
  }

  $scope.isClassValid = function($className) {
    return $className in $scope.keyspace.Classes;
  };

  $scope.isVindexValid = function($vindexName) {
    return $vindexName in $scope.keyspace.Vindexes;
  };
}
