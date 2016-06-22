app.controller('KeyspacesCtrl', function($scope, keyspaces, topodata, srvkeyspace, shards, actions) {
  $scope.keyspaceActions = [
    {name: 'ValidateKeyspace', title: 'Validate Keyspace'},
    {name: 'ValidateSchemaKeyspace', title: 'Validate Schema'},
    {name: 'ValidateVersionKeyspace', title: 'Validate Version'},
    {name: 'ValidatePermissionsKeyspace', title: 'Validate Permissions'},
  ];

  $scope.actions = actions;
  $scope.activeShards = [];

  $scope.isActive = function(name) {
    return $scope.activeShards.includes(name);
  }

  $scope.refreshData = function() {
    // Get list of keyspace names.

    srvkeyspace.get({cell_keyspace: "test/test_keyspace"}).$promise.then(function(value) {
      var partition = value.partitions[0];
      var shard_references = partition.shard_references;
      var num_shards = shard_references.length;
      for (var i = 0; i < num_shards; i++) {
        $scope.activeShards.push(shard_references[i].name);
      }
      console.log($scope.activeShards);
    });
    

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
