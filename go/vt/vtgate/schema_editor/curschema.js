/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function curSchema(vindexInfo) {
  var data = {};
  data.original = {
      "user": {
          "Sharded": true,
          "Vindexes": {
              "user_index": {
                  "Type": "hash_autoinc",
                  "Owner": "user",
                  "Params": {
                      "Table": "user_lookup",
                      "Column": "user_id"
                  }
              },
              "music_user_map": {
                  "Type": "lookup_hash_unique_autoinc",
                  "Owner": "music_extra",
                  "Params": {
                      "Table": "music_user_map",
                      "From": "music_id",
                      "To": "user_id"
                  }
              },
              "name_user_map": {
                  "Type": "lookup_hash",
                  "Owner": "user",
                  "Params": {
                      "Table": "name_user_map",
                      "From": "name",
                      "To": "user_id"
                  }
              },
              "user_extra_index": {
                "Type": "hash",
                "Owner": "user_extra",
                "Params": {
                    "Table": "user_extra_lookup",
                    "Column": "user_extra_id"
                }
            },
              "very_very_long_name": {
                "Type": "hash",
                "Owner": "very_very_long_name",
                "Params": {
                    "Table": "user_extra_lookup",
                    "Column": "user_extra_id"
                }
            }
          },
          "Classes": {
              "user": [
                  {
                      "Col": "id",
                      "Name": "user_index"
                  }, {
                      "Col": "",
                      "Name": "name_user_map"
                  }, {
                      "Col": "third",
                      "Name": "name_user_map"
                  }
              ],
              "user_extra": [
                  {
                      "Col": "user_id",
                      "Name": "user_index"
                  }, {
                      "Col": "id",
                      "Name": "user_extra_index"
                  }
              ],
              "music": [
                  {
                      "Col": "user_id",
                      "Name": "name_user_map"
                  }, {
                      "Col": "id",
                      "Name": "user_index"
                  }
              ],
              "music_extra": [
                              {
                                  "Col": "user_id",
                                  "Name": "music_user_map"
                              }, {
                                  "Col": "music_id",
                                  "Name": "user_index1"
                              }
              ],
          "very_very_long_name": [
                          {
                              "Col": "user_id",
                              "Name": "music_user_map"
                          }, {
                              "Col": "music_id",
                              "Name": "user_index1"
                          }
          ]
          },
          "Tables": {
              "user": "aa",
              "user_extra": "user_extra",
              "music": "music",
              "music_extra": "music_extra",
              "very_very_long_name": "music_extra"
          }
      },
      "very_very_long_name": {
        "Tables": {
            "main1": "aa",
            "main2": "",
            "music_extra": ""
        }
      }
  };

  data.reset = function() {
    data.keyspaces = copyKeyspaces(data.original, vindexInfo);
    data.tables = computeTables(data.keyspaces);
  };

  data.deleteKeyspace = function(keyspaceName) {
    delete data.keyspaces[keyspaceName];
    data.tables = computeTables(data.keyspaces);
  };

  data.addTable = function(keyspaceName, tableName, className) {
    data.keyspaces[keyspaceName].Tables[tableName] = className;
    data.tables = computeTables(data.keyspaces);
  };

  data.deleteTable = function(keyspaceName, tableName) {
    delete data.keyspaces[keyspaceName].Tables[tableName];
    data.tables = computeTables(data.keyspaces);
  };

  data.validClasses = function(keyspace, tableName) {
    var valid = [];
    if (!keyspace) {
      return [];
    }
    for ( var className in keyspace.Classes) {
      if (data.classHasError(keyspace, tableName, className)) {
        continue;
      }
      valid.push(className);
    }
    return valid;
  };

  data.classHasError = function(keyspace, tableName, className) {
    if (!(className in keyspace.Classes)) {
      return "class not found";
    }
    var klass = keyspace.Classes[className];
    for (var i = 0; i < klass.length; i++) {
      var classError = data.vindexHasError(keyspace, className, i);
      if (classError) {
        return "invalid class";
      }
      var vindex = keyspace.Vindexes[klass[i].Name];
      if (vindex.Owner != tableName) {
        continue;
      }
      if (i == 0) {
        if (vindexInfo.Types[vindex.Type].Type != "functional") {
          return "owned primary vindex must be functional";
        }
      } else {
        if (vindexInfo.Types[vindex.Type].Type != "lookup") {
          return "owned non-primary vindex must be lookup";
        }
      }
    }
    return "";
  };

  data.validVindexes = function(keyspace, className, index) {
    var valid = [];
    for ( var vindexName in keyspace.Vindexes) {
      // Duplicated from vindexHasError.
      if (index == 0) {
        var vindexTypeName = keyspace.Vindexes[vindexName].Type;
        if (!vindexInfo.Types[vindexTypeName].Unique) {
          continue;
        }
      }
      valid.push(vindexName);
    }
    return valid;
  };

  data.vindexHasError = function(keyspace, className, index) {
    var vindexName = keyspace.Classes[className][index].Name;
    if (!(vindexName in keyspace.Vindexes)) {
      return "vindex not found";
    }
    if (index == 0) {
      var vindexTypeName = keyspace.Vindexes[vindexName].Type;
      if (!vindexInfo.Types[vindexTypeName].Unique) {
        return "primary vindex must be unique";
      }
    }
    return "";
  };

  data.reset();
  return data;
}

function SetSharded(keyspace, sharded) {
  if (sharded) {
    keyspace.Sharded = true;
    if (!keyspace["Classes"]) {
      keyspace.Classes = {};
    }
    if (!keyspace["Vindexes"]) {
      keyspace.Vindexes = {};
    }
  } else {
    keyspace.Sharded = false;
    for ( var tableName in keyspace.Tables) {
      keyspace.Tables[tableName] = "";
    }
    delete keyspace["Classes"];
    delete keyspace["Vindexes"];
  }
};

function AddKeyspace(keyspaces, keyspaceName, sharded) {
  var keyspace = {};
  keyspace.Tables = {};
  SetSharded(keyspace, sharded);
  keyspaces[keyspaceName] = keyspace;
};

function CopyParams(original, type, vindexInfo) {
  var params = {};
  var vparams = vindexInfo.Types[type].Params;
  for (var i = 0; i < vparams.length; i++) {
    params[vparams[i]] = original[vparams[i]];
  }
  return params;
}

function copyKeyspaces(original, vindexInfo) {
  var copied = {};
  for ( var key in original) {
    copied[key] = {};
    var keyspace = copied[key];
    if (original[key].Sharded) {
      keyspace.Sharded = true;
      keyspace.Vindexes = copyVindexes(original[key].Vindexes, vindexInfo);
      keyspace.Classes = copyClasses(original[key].Classes);
      keyspace.Tables = copyTables(original[key].Tables);
    } else {
      keyspace.Sharded = false;
      keyspace.Tables = {};
      for (key in original[key].Tables) {
        keyspace.Tables[key] = "";
      }
    }
  }
  return copied;
}

function copyVindexes(original, vindexInfo) {
  var copied = {};
  for ( var key in original) {
    if (!vindexInfo.Types[original[key].Type]) {
      continue;
    }
    copied[key] = {};
    var vindex = copied[key];
    vindex.Type = original[key].Type;
    vindex.Owner = original[key].Owner;
    vindex.Params = CopyParams(original[key].Params, original[key].Type, vindexInfo);
  }
  return copied;
}

function copyClasses(original) {
  var copied = {};
  for ( var key in original) {
    copied[key] = [];
    for (var i = 0; i < original[key].length; i++) {
      copied[key].push({
          "Col": original[key][i].Col,
          "Name": original[key][i].Name
      });
    }
  }
  return copied;
}

function copyTables(original) {
  var copied = {};
  for ( var key in original) {
    copied[key] = original[key];
  }
  return copied;
}

function computeTables(keyspaces) {
  var tables = {};
  for ( var ks in keyspaces) {
    for ( var table in keyspaces[ks].Tables) {
      if (table in tables) {
        tables[table].push(ks);
      } else {
        tables[table] = [
          ks
        ];
      }
    }
  }
  return tables;
}

