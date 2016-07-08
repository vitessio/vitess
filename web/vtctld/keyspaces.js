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

  // Cleaning the data received to ensure that it has only keyspace names.
  // Replicates the functionality of the $resource.toJSON method because
  // it is unsupported on this version of Angular.
  function stripResource(data) {
    delete data.$promise;
    delete data.$resolved;
    return data;
  }

  $scope.refreshData = function() {
    var SrvKeyspacePromise = getSrvKeyspaces("local", "");
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
        keyspace.shards = getKeyspaceShards(keyspace, SrvKeyspacePromise);
        $scope.keyspaces.push(keyspace);
      });
    });
  };
  $scope.refreshData();

  function getKeyspaceShards(keyspace, SrvKeyspacePromise) {
    shards.query({keyspace: keyspace.name}, function(shardList){
      // Chain this callback onto the promise return from getSrvKeyspaces.
      SrvKeyspacePromise.finally(function(resp) {
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
  
  /* Refresh Serving Shards Helper Functions */

  // Fetch set of serving shards and return promise for the completion of
  // the request.
  function getSrvKeyspaces(cell, keyspace) {  
    return srv_keyspace.get({cell: cell, keyspace: keyspace}).$promise.then(function(resp) {
      var srvKeyspaces = Object.assign({}, resp);
      srvKeyspaces = stripResource(srvKeyspaces);
      if(keyspace == "") {
        processSrvkeyspaces(srvKeyspaces);
      } else {
        var srvKeyspace = srvKeyspaces;
        $scope.servingShards[keyspace] = {};
        addServingShards(srvKeyspace.partitions, keyspace);
      }
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
    // Use master partition SrvKeyspace to find serving shards.
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
    // Populates the object which is used as a set with the serving shard
    // names. Value is set to true as a place holder.
    for (var s = 0; s < shard_references.length; s++) {
      $scope.servingShards[keyspaceName][shard_references[s].name] = true;
    }
  }

  /* End Of Refresh Serving Shards Helper Functions */
});
