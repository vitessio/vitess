/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function curSchema() {
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
                  "Owner": "music",
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
              }
          },
          "Classes": {
              "user": [
                  {
                      "Col": "id",
                      "Name": "user_index"
                  }, {
                      "Col": "name",
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
                }
              ],
              "music": [
                  {
                      "Col": "user_id",
                      "Name": "user_index1"
                  }, {
                      "Col": "id",
                      "Name": "music_user_map"
                  }
              ],
              "music_extra": [
                  {
                      "Col": "user_id",
                      "Name": "user_index"
                  }, {
                      "Col": "music_id",
                      "Name": "music_user_map"
                  }
              ]
          },
          "Tables": {
              "user": "",
              "user_extra": "user_extra",
              "music": "music",
              "music_extra": "music_extra"
          }
      },
      "main": {
        "Tables": {
            "main1": "aa",
            "main2": ""
        }
      }
  };
  data.reset = function() {
    data.keyspaces = copyKeyspaces(data.original);
  };

  data.reset();
  return data;
}

function copyKeyspaces(original) {
  var copied = {};
  for ( var key in original) {
    copied[key] = {};
    var keyspace = copied[key];
    if (original[key].Sharded) {
      keyspace.Sharded = true;
      keyspace.Vindexes = copyVindexes(original[key].Vindexes);
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

function copyVindexes(original) {
  var copied = {};
  for ( var key in original) {
    copied[key] = {};
    var vindex = copied[key];
    vindex.Type = original[key].Type;
    vindex.Owner = original[key].Owner;
    switch (vindex.Type) {
      case "hash":
      case "hash_autoinc":
        vindex.Params = {};
        vindex.Params.Table = original[key].Params.Table;
        vindex.Params.Column = original[key].Params.Column;
        break;
      case "lookup_hash":
      case "lookup_hash_unique":
      case "lookup_hash_autoinc":
      case "lookup_hash_unique_autoinc":
        vindex.Params = {};
        vindex.Params.Table = original[key].Params.Table;
        vindex.Params.From = original[key].Params.From;
        vindex.Params.To = original[key].Params.To;
        break;
    }
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
  SetSharded(keyspace, sharded);
  keyspaces[keyspaceName] = keyspace;
};
