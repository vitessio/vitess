/**
 * Copyright 2015, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function DemoController($scope, $http) {

  function init() {
    $scope.samples = [
        "insert into user(name) values('test1') /* run this at least 6 times with different values of name */",
        "insert into user(name) values(null) /* error: name must be supplied */",
        "select user_id, name from user where user_id=6 /* unique select */",
        "select user_id, name from user where name='test1' /* non-unique select */",
        "select user_id, name from user where user_id in (1, 6) /* unique multi-select */",
        "select user_id, name from user where name in ('test1', 'test2') /* non-unique multi-select */",
        "select user_id, name from user /* scatter */",
        "select count(*) from user where user_id=1 /* aggregation on unique vindex */",
        "select count(*) from user where name='foo' /* error: aggregation on non-unique vindex */",
        "select user_id, name from user where user_id=1 limit 1 /* limit on unique vindex */",
        "update user set user_id=1 where user_id=2 /* error: cannot change vindex columns */",
        "delete from user where user_id=1 /* other 'test1' in name_user_idx unaffected */",
        "delete from user where name='test1' /* error: cannot delete by non-unique vindex */",
        "",
        "insert into user_extra(user_id, extra) values(1, 'extra1')",
        "insert into user_extra(user_id, extra) values(2, 'test1')",
        "insert into user_extra(user_id, extra) values(6, 'test2')",
        "insert into user_extra(extra) values('extra1') /* error: must supply value for user_id */",
        "select user_id, extra from user_extra where extra='extra1' /* scatter */",
        "update user_extra set extra='extra2' where user_id=1 /* allowed */",
        "delete from user_extra where user_id=1 /* vindexes are unchanged */",
        "",
        "insert into music(user_id) values(1) /* auto-inc on music_id */",
        "select user_id, music_id from music where user_id=1",
        "delete from music where music_id=6 /* one row deleted */",
        "delete from music where user_id=1 /* multiple rows deleted */",
        "",
        "insert into name_info(name, info) values('test1', 'test info')",
        "insert into name_info(name, info) values('test4', 'test info 4')",
        "select u.user_id, n.info from user u join name_info n on u.name=n.name where u.user_id=1",
        "",
        "select u.user_id, u.name, e.user_id, e.extra from user u join user_extra e on u.user_id = e.user_id where u.user_id = 2 /* simple, single row join */",
        "select u.user_id, u.name, e.user_id, e.extra from user u join user_extra e on u.user_id = e.user_id /* simple, scatter join */",
        "select u.user_id, u.name, e.user_id, e.extra from user u join user_extra e on u.name != e.extra where u.user_id = 2 /* simple, cross-shard complex join */",
        "select u1.user_id, u1.name, u2.user_id, u2.name from user u1 join user u2 on u1.name = u2.name where u1.user_id = 2 /* self-join */",
        "select u.user_id, u.name, e.user_id, e.extra from user u left join user_extra e on u.user_id = e.user_id /* left join */",
        "select u.user_id, u.name, e.user_id, e.extra from user u left join user_extra e on u.name != e.extra where u.user_id = 2 /* cross-shard left join */",
        "select user_id, name from user where name in (select extra from user_extra where user_id = user.user_id) /* correlated subquery */",
        "select count(*), u.user_id, u.name, e.extra from user u join user_extra e on u.user_id = e.user_id /* aggregates */",
        "select u.user_id, u.name, m.music_id, m.user_id from user u join music m on u.user_id = m.music_id where u.user_id = 1 order by u.user_id, u.name, m.user_id /* order by, in spite of odd join */",
        "",
        "insert into music_extra(music_id) values(1) /* keyspace_id back-computed */",
        "insert into music_extra(music_id, keyspace_id) values(1, 1) /* invalid keyspace id */",
    ];
    $scope.submitQuery()
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

  init();
}
