/**
 * Copyright 2015, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function SchemaManagerController($scope, $http) {
  init();

  function init() {
    $scope.schemaChanges = "";
    $scope.selectedKeyspace = "";
    $scope.shouldShowSchemaChangeStatus = false;
    $scope.keyspaces = [];
    $http.get('/json/Keyspaces').
    success(function(data, status, headers, config) {
      $scope.keyspaces = data.Keyspaces;
    }).
    error(function(data, status, headers, config) {
    });
  }

  $scope.selectKeyspace = function(selectedKeyspace) {
    $scope.selectedKeyspace = selectedKeyspace;
  }

  $scope.submitSchema = function() {
    $.ajax({
      type: 'POST',
      url: '/json/schema-manager',
      data: {"keyspace": $scope.selectedKeyspace, "data": $scope.schemaChanges},
      dataType: 'json'
    }).success(function(data) {
      $scope.schemaStatus = data.responseText;
      $scope.shouldShowSchemaChangeStatus = true;
      $scope.$apply();
    }).error(function(data) {
      $scope.schemaStatus = data.responseText;
      $scope.shouldShowSchemaChangeStatus = true;
      $scope.$apply();
    });
  };
}
