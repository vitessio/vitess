/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

function DemoController($scope, $http) {

  function init() {
    $scope.samples = [
      "insert into product(pname) values ('monitor'), ('keyboard')",
      "select * from product /* pass-through to unsharded keyspace */",
      "--",
      "insert into customer(uname) values('alice'),('bob'),('charlie'),('dan'),('eve') /* multi-shard insert */",
      "select * from customer where customer_id=1 /* use hash vindex */",
      "select * from customer where uname='bob' /* scatter */",
      "update customer set customer_id=10 where customer_id=1 /* error: cannot change primary vindex column */",
      "delete from customer where uname='eve' /* scatter DML */",
      "--",
      "insert into corder(customer_id, product_id, oname) values (1,1,'gift'),(1,2,'gift'),(2,1,'work'),(3,2,'personal'),(4,1,'personal') /* orders are grouped with their customer, observe lookup table changes */",
      "select * from corder where customer_id=1 /* single-shard select */",
      "select * from corder where corder_id=1 /* use unique lookup */",
      "select * from corder where oname='gift' /* use non-unique lookup, also try with 'work' and 'personal' */",
      "select c.uname, o.oname, o.product_id from customer c join corder o on c.customer_id = o.customer_id where c.customer_id=1 /* local join */",
      "select c.uname, o.oname, o.product_id from customer c join corder o on c.customer_id = o.customer_id /* scatter local join */",
      "select c.uname, o.oname, p.pname from customer c join corder o on c.customer_id = o.customer_id join product p on o.product_id = p.product_id /* cross-shard join */",
      "delete from corder where corder_id=3 /* delete also deletes lookup entries */",
      "--",
      "insert into corder_event(corder_id, ename) values(1, 'paid'), (5, 'delivered') /* automatic population of keyspace_id */",
    ];
    $scope.submitQuery()
  }

  $scope.submitQuery = function() {
    try {
      $http({
          method: 'POST',
          url: '/exec',
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
