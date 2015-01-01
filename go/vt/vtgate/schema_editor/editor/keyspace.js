/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function KeyspaceController($scope, $routeParams, vindexInfo, curSchema) {
  init();

  function init() {
    $scope.curSchema = curSchema;
    $scope.vindexInfo = vindexInfo;
    if (!$routeParams.keyspaceName || !curSchema.keyspaces[$routeParams.keyspaceName]) {
      return;
    }
    $scope.keyspaceName = $routeParams.keyspaceName;
    $scope.keyspace = curSchema.keyspaces[$routeParams.keyspaceName];
    if ($scope.keyspace.Sharded) {
      $scope.vindexNames = Object.keys($scope.keyspace.Vindexes);
    }
    $scope.tablesEditor = {};
    $scope.classesEditor = {};
    $scope.vindexEditor = {};
  }

  $scope.setSharded = function($sharded) {
    SetSharded($scope.keyspace, $sharded);
  };

  $scope.deleteKeyspace = function($keyspaceName) {
    curSchema.deleteKeyspace($keyspaceName);
  };

  $scope.tableHasError = function($tableName) {
    var table = curSchema.tables[$tableName];
    if (table && table.length > 1) {
      return $tableName + " duplicated in " + curSchema.tables[$tableName];
    }
  };

  $scope.addTable = function($tableName, $className) {
    if (!$tableName) {
      $scope.tablesEditor.err = "empty table name";
      return;
    }
    if ($tableName in curSchema.tables) {
      $scope.tablesEditor.err = $tableName + " already exists in " + curSchema.tables[$tableName];
      return;

    }
    curSchema.addTable($scope.keyspaceName, $tableName, $className);
    $scope.tablesEditor.newTableName = "";
    $scope.clearTableError();
  };

  $scope.setTableClass = function($tableName, $className) {
    $scope.keyspace.Tables[$tableName] = $className;
    $scope.clearTableError();
  };

  $scope.deleteTable = function($tableName) {
    curSchema.deleteTable($scope.keyspaceName, $tableName);
    $scope.clearTableError();
  };

  $scope.validClasses = function($tableName) {
    return curSchema.validClasses($scope.keyspace, $tableName);
  };

  $scope.classHasError = function($tableName, $className) {
    return curSchema.classHasError($scope.keyspace, $tableName, $className);
  };

  $scope.clearTableError = function() {
    $scope.tablesEditor.err = "";
  };

  $scope.addClass = function($className) {
    if (!$className) {
      $scope.classesEditor.err = "empty class name";
      return;
    }
    if ($className in $scope.keyspace.Classes) {
      $scope.classesEditor.err = $className + " already exists";
      return;
    }
    $scope.keyspace.Classes[$className] = [];
    $scope.classesEditor.newClassName = "";
    $scope.clearClassesError();
    window.location.href = "#/editor/" + $scope.keyspaceName + "/class/" + $className;
  };

  $scope.deleteClass = function($className) {
    delete $scope.keyspace.Classes[$className];
    $scope.clearClassesError();
  };

  $scope.clearClassesError = function() {
    $scope.classesEditor.err = "";
  };

  $scope.vindexHasError = function($className, $index) {
    return curSchema.vindexHasError($scope.keyspace, $className, $index);
  };

  $scope.setVindexType = function($vindex, $vindexType) {
    $vindex.Type = $vindexType;
    $vindex.Params = CopyParams($vindex.Params, $vindexType, vindexInfo);
    $scope.clearVindexError();
  };

  $scope.deleteVindex = function($vindexName) {
    delete $scope.keyspace.Vindexes[$vindexName];
    $scope.clearVindexError();
  };

  $scope.ownerHasWarning = function($owner, $vindexName) {
    if (!$owner) {
      return "";
    }
    var className = $scope.keyspace.Tables[$owner];
    if (!className) {
      return "table not found";
    }
    var klass = $scope.keyspace.Classes[className];
    if (!klass) {
      return "table has invalid class";
    }
    for (var i = 0; i < klass.length; i++) {
      if (klass[i].Name == $vindexName) {
        return "";
      }
    }
    return "table does not contain this index";
  };

  $scope.addVindex = function($vindexName, $vindexType) {
    if (!$vindexName) {
      $scope.vindexEditor.err = "empty vindex name";
      return;
    }
    if ($vindexName in $scope.keyspace.Vindexes) {
      $scope.vindexEditor.err = $vindexName + " already exists";
      return;
    }
    var newVindex = {
      "Params": {}
    };
    $scope.setVindexType(newVindex, $vindexType);
    $scope.keyspace.Vindexes[$vindexName] = newVindex;
    $scope.vindexEditor.vindexName = "";
    $scope.clearVindexError();
  };

  $scope.clearVindexError = function() {
    $scope.vindexEditor.err = "";
  };
}
