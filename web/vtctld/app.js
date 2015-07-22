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
    showInNav: true,
    icon: 'dashboard'
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
    showInNav: true,
    icon: 'storage'
  },
  {
    name: 'topo',
    title: 'Topology Browser',
    urlBase: '/topo/',
    urlPattern: '/topo/:path*?',
    templateUrl: 'topo.html',
    controller: 'TopoCtrl',
    showInNav: true,
    icon: 'folder'
  },
]);

app.config(function($mdThemingProvider, $routeProvider,
             $resourceProvider, routes) {
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

app.controller('ShardCtrl', function($scope, $routeParams, $timeout,
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

    {name: 'ScrapTablet', title: 'Scrap', confirm: 'This will tell the tablet to remove itself from serving.'},
    {name: 'ScrapTabletForce', title: 'Scrap (force)', confirm: 'This will externally remove the tablet from serving, without telling the tablet.'},
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

app.controller('TopoCtrl', function($scope, $routeParams, topodata) {
  var path = $routeParams.path;

  $scope.path = path;

  var crumbs = [];
  if (path) {
    var elems = path.split('/');
    while (elems.length > 0) {
      var elemPath = elems.join('/');
      crumbs.unshift({name: elems.pop(), path: elemPath});
    }
  }
  $scope.breadcrumbs = crumbs;

  $scope.refreshData = function() {
    $scope.node = topodata.get({path: path});
  };
  $scope.refreshData();
});

app.factory('actions', function($mdDialog, keyspaces, shards, tablets) {
  var svc = {};

  function actionDialogController($scope, $mdDialog, action, result) {
    $scope.hide = function() { $mdDialog.hide(); };
    $scope.action = action;
    $scope.result = result;
  }

  function showResult(ev, action, result) {
    $mdDialog.show({
      controller: actionDialogController,
      templateUrl: 'action-dialog.html',
      parent: angular.element(document.body),
      targetEvent: ev,
      locals: {
        action: action,
        result: result
      }
    });
  }

  function confirm(ev, action, doIt) {
    if (action.confirm) {
      var dialog = $mdDialog.confirm()
        .parent(angular.element(document.body))
        .title('Confirm ' + action.title)
        .content(action.confirm)
        .ariaLabel('Confirm action')
        .ok('OK')
        .cancel('Cancel')
        .targetEvent(ev);
      $mdDialog.show(dialog).then(doIt);
    } else {
      doIt();
    }
  }

  svc.applyKeyspace = function(ev, action, keyspace) {
    confirm(ev, action, function() {
      var result = keyspaces.action({
        keyspace: keyspace, action: action.name
      }, '');
      showResult(ev, action, result);
    });
  };

  svc.applyShard = function(ev, action, keyspace, shard) {
    confirm(ev, action, function() {
      var result = shards.action({
        keyspace: keyspace,
        shard: shard,
        action: action.name
        }, ''); 
      showResult(ev, action, result);
    });
  };

  svc.applyTablet = function(ev, action, tabletAlias) {
    confirm(ev, action, function() {
      var result = tablets.action({
        tablet: tabletAlias.Cell+'-'+tabletAlias.Uid,
        action: action.name
        }, '');
      showResult(ev, action, result);
    });
  };

  svc.label = function(action) {
    return action.confirm ? action.title + '...' : action.title;
  };

  return svc;
});

app.factory('cells', function($resource) {
  return $resource('/api/cells/');
});

app.factory('keyspaces', function($resource) {
  return $resource('/api/keyspaces/:keyspace', {}, {
      'action': {method: 'POST'}
  });
});

app.factory('shards', function($resource) {
  return $resource('/api/shards/:keyspace/:shard', {}, {
      'action': {method: 'POST'}
  });
});

app.factory('tablets', function($resource) {
  return $resource('/api/tablets/:tablet', {}, {
      'action': {method: 'POST'}
  });
});

app.factory('tabletinfo', function($resource) {
  return $resource('/api/tablets/:tablet/:info');
});

app.factory('endpoints', function($resource) {
  return $resource('/api/endpoints/:cell/:keyspace/:shard/:tabletType');
});

app.factory('topodata', function($resource) {
  return $resource('/api/topodata/:path');
});
