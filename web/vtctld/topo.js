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
