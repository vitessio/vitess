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

app.factory('srv_keyspace', function($resource) {
  return $resource('../api/srv_keyspace/:cell/:keyspace');
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
