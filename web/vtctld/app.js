app = angular.module('vtctld', ['ngMaterial', 'ngRoute', 'ngResource']);

vtTabletTypes = [
  'unknown', 'idle', 'master', 'replica', 'rdonly', 'spare', 'experimental',
  'schema_upgrade', 'backup', 'restore', 'worker', 'scrap'
];

app.constant('routes', [
  {
    urlPattern: '/',
    redirectTo: '/keyspaces/'
  },
  {
    name: 'keyspaces',
    title: 'Keyspaces',
    urlBase: '/keyspaces/',
    urlPattern: '/keyspaces/',
    templateUrl: 'keyspaces.html',
    controller: 'KeyspacesCtrl',
    showInNav: true
  },
  {
    name: 'shard',
    title: 'Shard Status',
    urlBase: '/shard/',
    urlPattern: '/shard/:keyspace/:shard',
    templateUrl: 'shard.html',
    controller: 'ShardCtrl'
  },
  {
    name: 'schema',
    title: 'Schema Manager',
    urlBase: '/schema/',
    urlPattern: '/schema/',
    templateUrl: 'schema.html',
    controller: 'SchemaCtrl',
    showInNav: true
  },
  {
    name: 'topo',
    title: 'Topology Browser',
    urlBase: '/topo/',
    urlPattern: '/topo/:path*?',
    templateUrl: 'topo.html',
    controller: 'TopoCtrl',
    showInNav: true
  },
]);

app.config(function($mdThemingProvider, $mdIconProvider, $routeProvider,
             $resourceProvider, routes) {
  $mdIconProvider
    .icon("menu", "img/menu.svg", 24)
    .icon("close", "img/close.svg", 24)
    .icon("refresh", "img/refresh.svg", 24)
    .icon("more_vert", "img/more_vert.svg", 24);

  $mdThemingProvider.theme('default')
    .primaryPalette('indigo')
    .accentPalette('red');

  routes.forEach(function(route) {
    $routeProvider.when(route.urlPattern, route);
  });

  $resourceProvider.defaults.stripTrailingSlashes = false;
});

app.controller('AppCtrl', function($scope, $mdSidenav, $route, $location,
                            routes) {
  $scope.routes = routes;

  $scope.refreshRoute = function() {
    $route.current.locals.$scope.refreshData();
  };

  $scope.toggleNav = function() { $mdSidenav('left').toggle(); }

  $scope.navIsSelected = function(item) {
    return $route.current && $route.current.name == item.name;
  };

  $scope.navTitle = function() {
    return $route.current ? $route.current.title : '';
  };

  $scope.navigate = function(path) {
    $location.path(path);
  };
});

app.controller('KeyspacesCtrl', function($scope, $mdDialog, keyspaces, shards) {
  $scope.keyspaceActions = [
    'Validate Keyspace',
    'Validate Permissions',
    'Validate Schema',
    'Validate Version'
  ];

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

app.controller('ShardCtrl', function($scope, $routeParams, $timeout,
                 shards, tablets, tabletinfo) {
  var keyspace = $routeParams.keyspace;
  var shard = $routeParams.shard;

  $scope.keyspace = {name: keyspace};
  $scope.shard = {name: shard};

  $scope.tabletActions = [
    'Ping', 'Scrap', 'Delete', 'Reload Schema'
  ];

  $scope.tabletType = function(tablet) {
    // Use streaming health result if present.
    if (tablet.streamHealth && tablet.streamHealth.$resolved) {
      if (!tablet.streamHealth.target)
        return 'spare';
      if (tablet.streamHealth.target.tablet_type)
        return vtTabletTypes[tablet.streamHealth.target.tablet_type];
    }
    return tablet.Type;
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
        if (cellMap[tabletAlias.Cell] === undefined)
          cellMap[tabletAlias.Cell] = [];

        cellMap[tabletAlias.Cell].push(tabletAlias);
      });

      // Turn the cell map into a list, sorted by cell name.
      var cellList = [];
      Object.keys(cellMap).sort().forEach(function(cellName) {
        // Sort the tablets within each cell.
        var tabletAliases = cellMap[cellName];
        tabletAliases.sort(function(a, b) { return a.Uid - b.Uid; });

        // Fetch tablet data.
        var tabletData = [];
        tabletAliases.forEach(function(tabletAlias) {
          var alias = tabletAlias.Cell+'-'+tabletAlias.Uid;

          var tablet = tablets.get({tablet: alias}, function(tablet) {
            // Annotate result with some extra stuff.
            tablet.links = vtconfig.tabletLinks(tablet);
          });
          tablet.Alias = tabletAlias;

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
        if (tablet.Alias) {
          // Get latest streaming health result.
          tabletinfo.get({tablet: tablet.Alias.Cell+'-'+tablet.Alias.Uid, info: 'health'}, function(health) {
            tablet.streamHealth = health;
          });
        }
      });
    }
  };

  $scope.refreshData();

  function periodicRefresh() {
    refreshStreamHealth();
    $timeout(periodicRefresh, 3000);
  }
  periodicRefresh();
});

app.controller('SchemaCtrl', function() {
});

app.controller('TopoCtrl', function($scope, $routeParams) {
  $scope.path = $routeParams.path;
});

app.factory('cells', function($resource) {
  return $resource('/api/cells/');
});

app.factory('keyspaces', function($resource) {
  return $resource('/api/keyspaces/:keyspace');
});

app.factory('shards', function($resource) {
  return $resource('/api/shards/:keyspace/:shard');
});

app.factory('tablets', function($resource) {
  return $resource('/api/tablets/:tablet');
});

app.factory('tabletinfo', function($resource) {
  return $resource('/api/tablets/:tablet/:info');
});

app.factory('endpoints', function($resource) {
  return $resource('/api/endpoints/:cell/:keyspace/:shard/:tabletType');
});
