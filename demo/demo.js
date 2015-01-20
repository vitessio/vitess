/**
 * Copyright 2015, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function DemoController($scope, $http) {
  init();

  function init() {
    $scope.samples = [
        "insert into user(name) values('test1') /* run this a few times */",
        "insert into user(user_id, name) values(6, 'test2') /* app-suplied user_id */",
        "insert into user(user_id, name) values(6, null) /* error: name must be supplied */",
        "select * from user where user_id=6 /* unique select */",
        "select * from user where name='test1' /* non-unique select */",
        "select * from user where user_id in (1, 6) /* unique multi-select */",
        "select * from user where name in ('test1', 'test2') /* non-unique multi-select */",
        "select * from user /* scatter */",
        "select count(*) from user where user_id=1 /* aggregation on unique vindex */",
        "select count(*) from user where name='foo' /* error: aggregation on non-unique vindex */",
        "select * from user where user_id=1 limit 1 /* limit on unique vindex */",
        "update user set user_id=1 where user_id=2 /* error: cannot change vindex columns */",
        "delete from user where user_id=1 /* other 'test1' in name_user_idx unaffected */",
        "delete from user where name='test1' /* error: cannot delete by non-unique vindex */",
        "",
        "insert into user_extra(user_id, extra) values(1, 'extra1')",
        "insert into user_extra(extra) values('extra1') /* error: must supply value for user_id */",
        "select * from user_extra where extra='extra1' /* scatter */",
        "update user_extra set extra='extra2' where user_id=1 /* allowed */",
        "delete from user_extra where user_id=1 /* vindexes are unchanged */",
        "",
        "insert into music(user_id) values(1) /* auto-inc on music_id */",
        "insert into music(user_id, music_id) values(1, 6) /* explicit music_id value */",
        "select * from music where user_id=1",
        "delete from music where music_id=6 /* one row deleted */",
        "delete from music where user_id=1 /* multiple rows deleted */",
        "",
        "insert into music_extra(music_id) values(1) /* keyspace_id back-computed */",
        "insert into music_extra(music_id, keyspace_id) values(1, 1) /* invalid keyspace id */",
    ];
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

  $scope.setQuery = function($query) {
    $scope.query = $query;
    angular.element("#query_input").focus();
  };
}