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
        tablet: tabletAlias.cell+'-'+tabletAlias.uid,
        action: action.name
        }, '');
      showResult(ev, action, result);
    });
  };

  svc.applyFunc = function(ev, action, func) {
    confirm(ev, action, function() {
      showResult(ev, action, func());
    });
  };

  svc.label = function(action) {
    return action.confirm ? action.title + '...' : action.title;
  };

  return svc;
});
