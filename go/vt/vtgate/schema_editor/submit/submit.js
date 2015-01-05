/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function SubmitController($scope, $http, curSchema) {
  init();

  function init() {
    $scope.keyspacesJSON = angular.toJson({
      "Keyspaces": curSchema.keyspaces
    }, true);
    $scope.originalJSON = angular.toJson({
      "Keyspaces": curSchema.original
    }, true);
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
        var parser = new DOMParser();
        var xmlDoc = parser.parseFromString(data, "text/xml");
        var err = xmlDoc.getElementById("err");
        if (err) {
          $scope.submitter.err = err.innerHTML;
          return;
        }
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
