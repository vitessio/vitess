vindexInfo = {};
vindexInfo.types = {
  "numeric": {
      "Type": "functional",
      "Unique": true,
      "Params": []
  },
  "hash": {
      "Type": "functional",
      "Unique": true,
      "Params": [
          "Table", "Column"
      ]
  },
  "hash_autoinc": {
      "Type": "functional",
      "Unique": true,
      "Params": [
          "Table", "Column"
      ]
  },
  "lookup_hash": {
      "Type": "lookup",
      "Unique": false,
      "Params": [
          "Table", "From", "To"
      ]
  },
  "lookup_hash_unique": {
      "Type": "lookup",
      "Unique": true,
      "Params": [
          "Table", "From", "To"
      ]
  },
  "lookup_hash_autoinc": {
      "Type": "lookup",
      "Unique": false,
      "Params": [
          "Table", "From", "To"
      ]
  },
  "lookup_hash_unique_autoinc": {
      "Type": "lookup",
      "Unique": true,
      "Params": [
          "Table", "From", "To"
      ]
  }
};
vindexInfo.typeNames = Object.keys(vindexInfo.types).sort();

app.controller('VSchemaCtrl', function($scope, $mdDialog,
               actions, vschema, keyspaces) {
  $scope.refreshData = function() {
    $scope.vschema = vschema.get();
    $scope.keyspaces = keyspaces.query();
  };
  $scope.refreshData();

  $scope.submitVSchema = function(ev) {
    var action = {
      title: 'Save VSchema',
      confirm: 'This will update the stored VSchema in topology.'
    };
    actions.applyFunc(ev, action, function() {
      var result = {$resolved: false};

      $scope.vschema.$save(
        function() {
          result.$resolved = true;
          result.Output = 'VSchema Saved';
          result.Error = false;
        },
        function(httpErr) {
          result.$resolved = true;
          result.Output = httpErr.data;
          result.Error = true;
        });

      return result;
    });
  };

  $scope.hasKeys = function(obj) {
    if (obj) {
      for (var key in obj)
        return true;
    }
    return false;
  };

  keyspaceSelector = function(searchText) {
    if (!searchText) return $scope.keyspaces;
    return $scope.keyspaces.filter(function(item) {
      return item.indexOf(searchText) != -1;
    });
  };

  $scope.classSelector = function(keyspace, searchText) {
    if (!keyspace.Classes) return [];
    var items = Object.keys(keyspace.Classes).sort();
    if (!searchText) return items;
    return items.filter(function(item) {
      return item.indexOf(searchText) != -1;
    });
  };

  $scope.vindexSelector = function(keyspace, searchText) {
    if (!keyspace.Vindexes) return [];
    var items = Object.keys(keyspace.Vindexes).sort();
    if (!searchText) return items;
    return items.filter(function(item) {
      return item.indexOf(searchText) != -1;
    });
  };

  $scope.vindexTypeSelector = function(searchText) {
    if (!searchText) return vindexInfo.typeNames;
    return vindexInfo.typeNames.filter(function(item) {
      return item.indexOf(searchText) != -1;
    });
  };

  $scope.setKeyspace = function(name, keyspace) {
    $scope.keyspace = keyspace;
    $scope.keyspacename = name;
  };

  function addKeyspace(keyspace) {
    if (keyspace) {
      if (!$scope.vschema.Keyspaces)
        $scope.vschema.Keyspaces = {};
      if (!$scope.vschema.Keyspaces[keyspace])
        $scope.vschema.Keyspaces[keyspace] = {};
    }
  }

  $scope.addKeyspaceDialog = function(ev) {
    $mdDialog.show({
      controller: function($scope, $mdDialog) {
        $scope.hide = function() { $mdDialog.hide(); };
        $scope.keyspace = '';
        $scope.addKeyspace = addKeyspace;
        $scope.keyspaceSelector = keyspaceSelector;
      },
      templateUrl: 'vschema/add-keyspace-dialog.html',
      parent: angular.element(document.body),
      targetEvent: ev
    });
  };

  $scope.addTable = function(keyspace, table, classname) {
    if (table) {
      if (!keyspace.Tables)
        keyspace.Tables = {};
      if (!keyspace.Tables[table])
        keyspace.Tables[table] = keyspace.Sharded ? classname : '';
    }
  };

  $scope.addClass = function(keyspace, classname) {
    if (classname) {
      if (!keyspace.Classes)
        keyspace.Classes = {};
      if (!keyspace.Classes[classname])
        keyspace.Classes[classname] = {ColVindexes: []};
    }
  };

  $scope.addColumn = function(keyspace, classname, col, vindex) {
    if (!keyspace.Classes[classname].ColVindexes)
      keyspace.Classes[classname].ColVindexes = [];
    keyspace.Classes[classname].ColVindexes.push(
      {Col: col, Name: vindex}
    );
  };

  $scope.addVindex = function(keyspace, vindex, type, owner) {
    if (vindex) {
      if (!keyspace.Vindexes)
        keyspace.Vindexes = {};
      if (!keyspace.Vindexes[vindex]) {
        keyspace.Vindexes[vindex] = {Type: type, Owner: owner};
        $scope.onVindexTypeChange(keyspace.Vindexes[vindex], type);
      }
    }
  };

  $scope.removeTable = function(keyspace, table) {
    delete keyspace.Tables[table];
  };

  $scope.removeKeyspace = function(keyspacename) {
    delete $scope.vschema.Keyspaces[keyspacename];
  };

  $scope.removeClass = function(keyspace, classname) {
    delete keyspace.Classes[classname];
  };

  $scope.removeColumn = function(keyspace, classname, index) {
    keyspace.Classes[classname].ColVindexes.splice(index, 1);
  };

  $scope.removeVindex = function(keyspace, vindex) {
    delete keyspace.Vindexes[vindex];
  };

  $scope.onShardedChange = function(keyspace) {
    if (keyspace.Sharded) {
      if (!keyspace.Classes)
        keyspace.Classes = {};
      if (!keyspace.Vindexes)
        keyspace.Vindexes = {};
    } else {
      delete keyspace.Classes;
      delete keyspace.Vindexes;
      for (var table in keyspace.Tables)
        keyspace.Tables[table] = '';
    }
  };

  $scope.onVindexTypeChange = function(vindex, type) {
    if (type in vindexInfo.types) {
      params = vindexInfo.types[type].Params;
      if (!vindex.Params)
        vindex.Params = {};

      // Remove params that shouldn't be there.
      for (var param in vindex.Params) {
        if (params.indexOf(param) == -1)
          delete vindex.Params[param];
      }
      // Add params that should be there but aren't.
      params.forEach(function (param) {
        if (!vindex.Params[param])
          vindex.Params[param] = '';
      });
    }
  };
});

app.directive('vclass', function() {
  return {
    require: 'ngModel',
    link: function(scope, elem, attrs, ctrl) {
      ctrl.$validators.vempty = function(modelValue, viewValue) {
        if (!scope.keyspace.Sharded)
          return viewValue == '';
        return true;
      };
      ctrl.$validators.vrequired = function(modelValue, viewValue) {
        if (scope.keyspace.Sharded)
          return !!viewValue;
        return true;
      };
      ctrl.$validators.vdefined = function(modelValue, viewValue) {
        if (viewValue && scope.keyspace.Sharded)
          return viewValue in scope.keyspace.Classes;
        return true;
      };
    }
  };
});

app.directive('vindex', function() {
  return {
    require: 'ngModel',
    link: function(scope, elem, attrs, ctrl) {
      ctrl.$validators.vdefined = function(modelValue, viewValue) {
        if (viewValue)
          return viewValue in scope.keyspace.Vindexes;
        return true;
      };
    }
  };
});

app.directive('vindexType', function() {
  return {
    require: 'ngModel',
    link: function(scope, elem, attrs, ctrl) {
      ctrl.$validators.vdefined = function(modelValue, viewValue) {
        if (viewValue)
          return viewValue in vindexInfo.types;
        return true;
      };
    }
  };
});
