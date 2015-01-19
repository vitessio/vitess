/**
 * Copyright 2014, Google Inc. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
'use strict';

function DemoController($scope, $http) {
  init();

  function init() {
  }
  
  $scope.submitQuery = function() {
    try {
      $http({
          method: 'POST',
          url: '/cgi-bin/data.py',
          data: "query=" + $scope.query,
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
      }).success(function(data, status, headers, config) {
        $scope.result = angular.fromJson(data);
      });
    } catch (err) {
      $scope.result.error = err.message;
    }
  };
}