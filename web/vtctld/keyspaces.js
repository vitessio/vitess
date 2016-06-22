app.controller('KeyspacesCtrl', function($scope, keyspaces, srv_keyspace, shards, actions) {
  $scope.keyspaceActions = [
    {name: 'ValidateKeyspace', title: 'Validate Keyspace'},
    {name: 'ValidateSchemaKeyspace', title: 'Validate Schema'},
    {name: 'ValidateVersionKeyspace', title: 'Validate Version'},
    {name: 'ValidatePermissionsKeyspace', title: 'Validate Permissions'},
  ];

  $scope.actions = actions;
  $scope.servingShards = {};

  $scope.isServing = function(name, keyspaceName) {
    return name in $scope.servingShards[keyspaceName];
  }

  $scope.refreshData = function() {
    //Refresh set of serving shards
    refreshServingShards("test", "");
    
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
  
  /*Refresh Serving Shards Helper Functions*/

  
  function refreshServingShards(cell, keyspace) {
    var target;
    //Correctly formatting the server parameter depending on keyspace
    if (keyspace == "") target = cell;
    else target = cell + "/" + keyspace;
    //Call to srv_keyspace API
    srv_keyspace.get({cell_keyspace: target}).$promise.then(function(resp) {
      var srvKeyspaceMap = resp.Data;
      var keyspaceNames = Object.keys(srvKeyspaceMap);
      processSrvkeyspaces(keyspaceNames, srvKeyspaceMap);
    });
  }


  function processSrvkeyspaces(keyspaceNames, srvKeyspaceMap){
    var num_srvKeyspaces = keyspaceNames.length;
    $scope.servingShards = {}; 
    for (var k = 0; k < num_srvKeyspaces; k++) {
      $scope.servingShards[keyspaceNames[k]] = {};
      var curr_srv_keyspace = srvKeyspaceMap[keyspaceNames[k]];
      addServingShards(curr_srv_keyspace.partitions, keyspaceNames[k]);
    }
  }

  function addServingShards(partitions, keyspaceName){
    var num_partitions = partitions.length;
    //Use master partition srvkeyspace to find serving shards
    for (var p = 0; p < num_partitions; p++) {
      var partition = partitions[p];
      if (partition.served_type == 1) { //MASTER = 1
        handleMasterPartion(partition, keyspaceName)
        break;
      }
    }
  }

  function handleMasterPartion(partition, keyspaceName){
    var shard_references = partition.shard_references;
    var num_shards = shard_references.length;
    //Populates the object which is used as a set with the serving shard 
    //names. Value is set to true as a place holder. 
    for (var s = 0; s < num_shards; s++) {
      $scope.servingShards[keyspaceName][shard_references[s].name] = true;
    }
  }

  /*End Of Refresh Serving Shards Helper Functions*/

});
