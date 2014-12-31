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
  }

  $scope.setSharded = function($sharded) {
    SetSharded($scope.keyspace, $sharded);
  };

  $scope.deleteKeyspace = function($keyspaceName) {
    delete curSchema.keyspaces[$keyspaceName];
  };

  $scope.tableHasError = function($tableName) {
    if (curSchema.tables[$tableName].length > 1) {
      return $tableName + " duplicated in " + curSchema.tables[$tableName];
    }
  }

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
    var valid = [];
    for ( var className in $scope.keyspace.Classes) {
      if ($scope.classHasError($tableName, className)) {
        continue;
      }
      valid.push(className);
    }
    return valid;
  };

  $scope.classHasError = function($tableName, $className) {
    if (!($className in $scope.keyspace.Classes)) {
      return "class not found";
    }
    var klass = $scope.keyspace.Classes[$className];
    for (var i = 0; i < klass.length; i++) {
      var classError = $scope.vindexHasError($className, i);
      if (classError) {
        return "invalid class";
      }
      var vindex = $scope.keyspace.Vindexes[klass[i].Name];
      if (vindex.Owner != $tableName) {
        continue;
      }
      if (i == 0) {
        if (vindexInfo.Types[vindex.Type].Type != "functional") {
          return "owned primary vindex must be functional";
        }
      } else {
        if (vindexInfo.Types[vindex.Type].Type != "lookup") {
          return "owned non-primary vindex must be lookup";
        }
      }
    }
    return "";
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
    $scope.keyspace.Classes[$className] = {};
    $scope.classesEditor.newClassName = "";
    $scope.clearClassesError();
  };

  $scope.deleteClass = function($className) {
    delete $scope.keyspace.Classes[$className];
    $scope.clearClassesError();
  };

  $scope.clearClassesError = function() {
    $scope.classesEditor.err = "";
  };

  $scope.vindexHasError = function($className, $index) {
    var vindexName = $scope.keyspace.Classes[$className][$index].Name;
    if (!(vindexName in $scope.keyspace.Vindexes)) {
      return "vindex not found";
    }
    if ($index == 0) {
      var vindexTypeName = $scope.keyspace.Vindexes[vindexName].Type;
      if (!vindexInfo.Types[vindexTypeName].Unique) {
        return "primary vindex must be unique";
      }
    }
    return "";
  };
}
