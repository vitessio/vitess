app.controller('KeyspacesCtrl', function($scope, keyspaces, shards, actions) {
  $scope.keyspaceActions = [
    {name: 'ValidateKeyspace', title: 'Validate Keyspace'},
    {name: 'ValidateSchemaKeyspace', title: 'Validate Schema'},
    {name: 'ValidateVersionKeyspace', title: 'Validate Version'},
    {name: 'ValidatePermissionsKeyspace', title: 'Validate Permissions'},
  ];

  $scope.actions = actions;

  $scope.refreshData = function() {
    // Get list of keyspace names.
    keyspaces.query(function(ksnames) {
      $scope.keyspaces = [];
      ksnames.forEach(function(name) {
        // Get a list of shards for each keyspace.
        $scope.keyspaces.push({
          name: name,
          shards: shards.query({keyspace: name})
        });
      });
    });
  };
  $scope.refreshData();
});
