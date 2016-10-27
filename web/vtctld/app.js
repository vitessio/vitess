app = angular.module('vtctld', ['ngMaterial', 'ngRoute', 'ngResource', 'ngMessages']);

vtTabletTypes = [
  'unknown', 'master', 'replica', 'rdonly', 'spare', 'experimental',
  'backup', 'restore', 'drained'
];

app.constant('routes', [
  {
    urlPattern: '/',
    redirectTo: '/keyspaces/'
  },
  {
    name: 'keyspaces',
    title: 'Dashboard',
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
    name: 'topo',
    title: 'Topology Browser',
    urlBase: '/topo/',
    urlPattern: '/topo/:path*?',
    templateUrl: 'topo.html',
    controller: 'TopoCtrl',
    showInNav: true,
    icon: 'folder'
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
  }
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
    if ($route.current && $route.current.locals.$scope.refreshData)
      $route.current.locals.$scope.refreshData();
    else
      $route.reload();
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
