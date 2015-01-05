/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function SubmitController($scope, $http, curSchema) {
  init();

  function init() {
    $scope.keyspacesJSON = angular.toJson(curSchema.keyspaces, true);
    $scope.originalJSON = angular.toJson(curSchema.original, true);
    $scope.submitter = {};
  }

  $scope.submitToTopo = function() {
    try {
      $http({
          method: 'POST',
          url: '/vschema',
          data: "vschema=" + $scope.keyspacesJSON,
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
      }).success(function(data, status, headers, config) {
        $scope.submitter.ok = "Submitted!";
      });
    } catch (err) {
      $scope.submitter.err = err.message;
    }
  };

  $scope.clearSubmitter = function() {
    $scope.submitter.err = "";
    $scope.submitter.ok = "";
  };
}
