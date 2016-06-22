app.factory('cells', function($resource) {
  return $resource('../api/cells/');
});

app.factory('keyspaces', function($resource) {
  return $resource('../api/keyspaces/:keyspace', {}, {
      'action': {method: 'POST'}
  });
});

app.factory('shards', function($resource) {
  return $resource('../api/shards/:keyspace/:shard', {}, {
      'action': {method: 'POST'}
  });
});

app.factory('srvkeyspace', function($resource) {
  return $resource('../api/srvkeyspace/:cell_keyspace');
});

app.factory('tablets', function($resource) {
  return $resource('../api/tablets/:tablet', {}, {
      'action': {method: 'POST'}
  });
});

app.factory('tabletinfo', function($resource) {
  return $resource('../api/tablets/:tablet/:info');
});

app.factory('topodata', function($resource) {
  return $resource('../api/topodata/:path');
});

app.factory('vschema', function($resource) {
  return $resource('../api/vschema/');
});
