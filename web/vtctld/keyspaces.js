app.controller('KeyspacesCtrl', function($scope, keyspaces, srv_keyspace, shards, actions) {
  $scope.keyspaceActions = [
    {name: 'ValidateKeyspace', title: 'Validate Keyspace'},
    {name: 'ValidateSchemaKeyspace', title: 'Validate Schema'},
    {name: 'ValidateVersionKeyspace', title: 'Validate Version'},
    {name: 'ValidatePermissionsKeyspace', title: 'Validate Permissions'},
  ];

  $scope.actions = actions;
  $scope.servingShards = {};

  $scope.hasServingData = function(keyspace) {
    return keyspace in $scope.servingShards;
  }

  $scope.isServing = function(keyspaceName, shardName) {
    return keyspaceName in $scope.servingShards && 
           shardName in $scope.servingShards[keyspaceName];
  }

  //cleaning the data recieved to ensure that it has only keyspace names
  function stripResource(list) {
    var data = list;
    delete data.$promise;
    delete data.$resolved;
    return data;
  }

  $scope.refreshData = function() {
    //Refresh set of serving shards
    refreshSrvKeyspaces("local", "");
    // Get list of keyspace names.
    keyspaces.query(function(ksnames) {
      $scope.keyspaces = [];
      ksnames.forEach(function(name) {
        var keyspace = {
          servingShards: [],
          nonServingShards: [],
          name: name,
          shards: [],
        }
        // Get a list of serving and nonserving shards from each keyspace.
        keyspace.shards = getKeyspaceShards(keyspace, name);
        $scope.keyspaces.push(keyspace);
      });
    });
  };
  $scope.refreshData();
  
  function getKeyspaceShards(keyspace) {
    shards.query({keyspace: keyspace.name}, function(shardList){
      // Chain this callback onto the srv_keyspace_query, since we need the
      //result from that as well.
      $scope.srv_keyspace_query.$promise.then(function(resp) {
        shardList = stripResource(shardList);
        for (var i = 0; i < shardList.length; i++) {
          if($scope.isServing(keyspace.name, shardList[i])) {
             keyspace.servingShards.push(shardList[i]);
          } else {
            keyspace.nonServingShards.push(shardList[i]);
          }
        }
      })
    });
  }
  
  /*Refresh Serving Shards Helper Functions*/
  function refreshSrvKeyspaces(cell, keyspace) {
    //Correctly formatting the server parameter depending on keyspace
    $scope.srv_keyspace_query = srv_keyspace.get({cell: cell, keyspace: keyspace});
    $scope.srv_keyspace_query.$promise.then(function(resp) {
      var srvKeyspaces = Object.assign({}, resp);
      srvKeyspaces = stripResource(srvKeyspaces);
      processSrvkeyspaces(srvKeyspaces);
    });
  }


   function processSrvkeyspaces(srvKeyspaceMap){
    $scope.servingShards = {}; 
    for (var keyspaceName in srvKeyspaceMap) {
      $scope.servingShards[keyspaceName] = {};
      var curr_srv_keyspace = srvKeyspaceMap[keyspaceName];
      addServingShards(curr_srv_keyspace.partitions, keyspaceName);
    }
  }

  function addServingShards(partitions, keyspaceName){
    if(partitions === undefined) {
      return;
    }
    //Use master partition srvkeyspace to find serving shards
    for (var p = 0; p < partitions.length; p++) {
      var partition = partitions[p];
      if (vtTabletTypes[partition.served_type] == 'master') {
        handleMasterPartion(partition, keyspaceName)
        break;
      }
    }
  }

  function handleMasterPartion(partition, keyspaceName){
    var shard_references = partition.shard_references; 
    if(shard_references === undefined) {
      return;
    }
    //Populates the object which is used as a set with the serving shard 
    //names. Value is set to true as a place holder. 
    for (var s = 0; s < shard_references.length; s++) {
      $scope.servingShards[keyspaceName][shard_references[s].name] = true;
    }
  }

  /*End Of Refresh Serving Shards Helper Functions*/

});
