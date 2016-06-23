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

  $scope.isServing = function(name, keyspaceName) {
    return name in $scope.servingShards[keyspaceName];
  }

  //cleaning the data recieved to ensure that it has only keyspace names
  function stripResource(shardList) {
    var data = shardList;
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
  
  function getKeyspaceShards(keyspace, keyspaceName) {
    shards.query({keyspace: keyspaceName}, function(shardList){
      $scope.srv_keyspace_query.$promise.then(function(resp) {
        shardList = stripResource(shardList);
        var num_shards = shardList.length;
        for (var i = 0; i < num_shards; i++) {
          if($scope.isServing(shardList[i], keyspaceName)) {
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
