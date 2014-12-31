/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function KeyspaceController($scope, $routeParams, vindexInfo, curSchema) {
  init();

  function init() {
    $scope.keyspaces = curSchema.keyspaces;
    $scope.vindexTypes = vindexInfo.TypeNames;
    if (!$routeParams.keyspaceName || !$scope.keyspaces[$routeParams.keyspaceName]) {
      return;
    }
    $scope.keyspaceName = $routeParams.keyspaceName;
    $scope.keyspace = $scope.keyspaces[$routeParams.keyspaceName];
    if ($scope.keyspace.Sharded) {
      $scope.vindexNames = Object.keys($scope.keyspace.Vindexes);
    }
    $scope.tablesEditor = {};
  }

  $scope.setSharded = function($sharded) {
    SetSharded($scope.keyspace, $sharded);
  };

  $scope.deleteKeyspace = function($keyspaceName) {
    delete $scope.keyspaces[$keyspaceName];
  };

  $scope.addTable = function($tableName, $className) {
    if (!$tableName) {
      $scope.tablesEditor.err = "empty table name";
      return;

    }
    if ($tableName in $scope.keyspace.Tables) {
      $scope.tablesEditor.err = $tableName + " already exists";
      return;

    }
    $scope.setTableClass($tableName, $className);
    $scope.tablesEditor.newTableName = "";
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

  $scope.isClassValid = function($className) {
    return $className in $scope.keyspace.Classes;
  };

  $scope.clearTableError = function() {
    $scope.tablesEditor.err = "";
  };

  $scope.addClass = function($className) {
    $scope.keyspace.Classes[$className] = {};
  };

  $scope.isVindexValid = function($vindexName) {
    return $vindexName in $scope.keyspace.Vindexes;
  };
}
