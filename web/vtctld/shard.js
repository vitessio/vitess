app.controller('ShardCtrl', function($scope, $routeParams, $timeout, $route,
                 shards, tablets, tabletinfo, actions) {
  var keyspace = $routeParams.keyspace;
  var shard = $routeParams.shard;

  $scope.keyspace = {name: keyspace};
  $scope.shard = {name: shard};
  $scope.actions = actions;

  $scope.shardActions = [
    {name: 'ValidateShard', title: 'Validate Shard'},
    {name: 'ValidateSchemaShard', title: 'Validate Schema'},
    {name: 'ValidateVersionShard', title: 'Validate Version'},
    {name: 'ValidatePermissionsShard', title: 'Validate Permissions'},
  ];

  $scope.tabletActions = [
    {name: 'Ping', title: 'Ping'},

    {name: 'RefreshState', title: 'Refresh State', confirm: 'This will tell the tablet to re-read its topology record and adjust its state accordingly.'},
    {name: 'ReloadSchema', title: 'Reload Schema', confirm: 'This will tell the tablet to refresh its schema cache by querying mysqld.'},

    {name: 'DeleteTablet', title: 'Delete', confirm: 'This will delete the tablet record from topology.'},
  ];

  $scope.tabletType = function(tablet) {
    // Use streaming health result if present.
    if (tablet.streamHealth && tablet.streamHealth.$resolved) {
      if (!tablet.streamHealth.target)
        return 'spare';
      if (tablet.streamHealth.target.tablet_type)
        return vtTabletTypes[tablet.streamHealth.target.tablet_type];
    }
    return vtTabletTypes[tablet.type];
  };

  $scope.tabletHealthError = function(tablet) {
    if (tablet.streamHealth && tablet.streamHealth.realtime_stats
        && tablet.streamHealth.realtime_stats.health_error) {
      return tablet.streamHealth.realtime_stats.health_error;
    }
    return '';
  };

  $scope.tabletAccent = function(tablet) {
    if ($scope.tabletHealthError(tablet))
      return 'md-warn md-hue-2';

    switch ($scope.tabletType(tablet)) {
      case 'master': return 'md-hue-2';
      case 'replica': return 'md-hue-3';
      default: return 'md-hue-1';
    }
  };

  $scope.refreshData = function() {
    // Get the shard data.
    shards.get({keyspace: keyspace, shard: shard}, function(shardData) {
      shardData.name = shard;
      $scope.shard = shardData;
    });

    // Get a list of tablet aliases in the shard, in all cells.
    tablets.query({shard: keyspace+'/'+shard}, function(tabletAliases) {
      // Group them by cell.
      var cellMap = {};
      tabletAliases.forEach(function(tabletAlias) {
        if (cellMap[tabletAlias.cell] === undefined)
          cellMap[tabletAlias.cell] = [];

        cellMap[tabletAlias.cell].push(tabletAlias);
      });

      // Turn the cell map into a list, sorted by cell name.
      var cellList = [];
      Object.keys(cellMap).sort().forEach(function(cellName) {
        // Sort the tablets within each cell.
        var tabletAliases = cellMap[cellName];
        tabletAliases.sort(function(a, b) { return a.uid - b.uid; });

        // Fetch tablet data.
        var tabletData = [];
        tabletAliases.forEach(function(tabletAlias) {
          var alias = tabletAlias.cell+'-'+tabletAlias.uid;

          var tablet = tablets.get({tablet: alias}, function(tablet) {
            // Annotate result with some extra stuff.
            tablet.links = vtconfig.tabletLinks(tablet);
          });
          tablet.alias = tabletAlias;

          tabletData.push(tablet);
        });

        // Add tablet data to the cell list.
        cellList.push({
          name: cellName,
          tablets: tabletData
        });
      });
      $scope.cells = cellList;
    });
  };

  var selectedCell;

  $scope.setSelectedCell = function(cell) {
    selectedCell = cell;
    refreshStreamHealth();
  };

  function refreshStreamHealth() {
    if (selectedCell) {
      selectedCell.tablets.forEach(function (tablet) {
        if (tablet.alias) {
          // Get latest streaming health result.
          tabletinfo.get({tablet: tablet.alias.cell+'-'+tablet.alias.uid, info: 'health'}, function(health) {
            tablet.streamHealth = health;
          });
        }
      });
    }
  };

  $scope.refreshData();

  function periodicRefresh() {
    if ($route.current.name != 'shard') return;
    refreshStreamHealth();
    $timeout(periodicRefresh, 3000);
  }
  periodicRefresh();
});
