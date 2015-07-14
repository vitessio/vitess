app = angular.module('vtctld', ['ngMaterial', 'ngRoute', 'ngResource']);

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
	$scope.reload = $route.reload;

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
});

app.controller('ShardCtrl', function($scope, $routeParams, shards, tablets) {
	keyspace = $routeParams.keyspace;
	shard = $routeParams.shard;

	$scope.keyspace = {name: keyspace};
	$scope.shard = {name: shard};

	$scope.tabletActions = [
		'Ping', 'Scrap', 'Delete', 'Reload Schema'
	];

	$scope.tabletAccent = function(tablet) {
		switch (tablet.Type) {
			case 'master': return 'md-warn md-hue-3';
			case 'replica': return 'md-hue-3';
			default: return 'md-hue-1';
		}
	};

	// Get the shard data.
	shards.get({keyspace: keyspace, shard: shard}, function(shardData) {
		shardData.name = shard;
		$scope.shard = shardData;
	});

	// Get a list of tablet aliases in the shard, in all cells.
	tablets.query({shard: keyspace+'/'+shard}, function(tabletAliases) {
		// Group them by cell.
		cellMap = {};
		tabletAliases.forEach(function(tabletAlias) {
			if (cellMap[tabletAlias.Cell] === undefined)
				cellMap[tabletAlias.Cell] = [];

			cellMap[tabletAlias.Cell].push(tabletAlias);
		});

		// Turn the cell map into a list, sorted by cell name.
		cellList = [];
		$scope.cells = cellList;
		Object.keys(cellMap).sort().forEach(function(cellName) {
			// Sort the tablets within each cell.
			tabletAliases = cellMap[cellName];
			tabletAliases.sort(function(a, b) { return a.Uid - b.Uid; });

			// Fetch tablet data.
			tabletData = [];
			tabletAliases.forEach(function(tabletAlias) {
				tabletData.push(
					tablets.get({tablet: tabletAlias.Cell+'-'+tabletAlias.Uid}, function(tablet) {
						// Annotate result with some extra stuff.
						tablet.links = vtconfig.tabletLinks(tablet);
					})
				);
			});

			// Add tablet data to the cell list.
			cellList.push({
				name: cellName,
				tablets: tabletData
			});
		});
	});
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

app.factory('endpoints', function($resource) {
	return $resource('/api/endpoints/:cell/:keyspace/:shard/:tabletType');
});
