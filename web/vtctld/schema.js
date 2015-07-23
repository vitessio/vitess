app.controller('SchemaCtrl', function($scope, $http, $mdDialog,
               actions, keyspaces) {
  $scope.schemaChange = {Keyspace: '', SQL: ''};

  $scope.refreshData = function() {
    $scope.keyspaces = keyspaces.query();
  };
  $scope.refreshData();

  $scope.submitSchema = function(ev) {
    var action = {
      title: 'Apply Schema',
      confirm: 'This will execute the provided SQL on all shards in the keyspace.'
    };
    actions.applyFunc(ev, action, function() {
      var result = {$resolved: false};

      $http.post('/api/schema/apply', $scope.schemaChange)
        .success(function(data) {
          result.$resolved = true;
          result.Output = data;
          result.Error = false;
        })
        .error(function(data) {
          result.$resolved = true;
          result.Output = data;
          result.Error = true;
        });

      return result;
    });
  };
});
