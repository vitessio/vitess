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
          "Tables": {
              "user": {
                "ColVindexes": [
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
                ]
              },
              "user_extra": {
                "ColVindexes": [
                  {
                      "Col": "user_id",
                      "Name": "user_index"
                  }
                ]
              },
              "music": {
                "ColVindexes": [
                    {
                        "Col": "user_id",
                        "Name": "user_index1"
                    }, {
                        "Col": "id",
                        "Name": "music_user_map"
                    }
                ]
              },
              "music_extra": {
                "ColVindexes": [
                    {
                        "Col": "user_id",
                        "Name": "user_index"
                    }, {
                        "Col": "music_id",
                        "Name": "music_user_map"
                    }
                ]
              }
          }
      },
      "main": {
        "Tables": {
          "main1": {}
        }
      }
  };
  data.keyspaces = copyKeyspaces(data.original);
  return data;
}

function copyKeyspaces(original) {
  var copied = {};
  for (var key in original) {
    copied[key] = {};
    var keyspace = copied[key];
    if (original[key].Sharded) {
      keyspace.Sharded = true;
      keyspace.Vindexes = copyVindexes(original[key].Vindexes);
      keyspace.Tables = copyShardedTables(original[key].Tables);
    } else {
      keyspace.Sharded = false;
      keyspace.Tables = copyUnshardedTables(original[key].Tables);
    }
  }
  return copied;
}

function copyVindexes(original) {
  var copied = {};
  for (var key in original) {
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

function copyShardedTables(original) {
  var copied = {};
  for (var key in original) {
    copied[key] = {};
    var table = copied[key];
    table.ColVindexes = copyColVindexes(original[key].ColVindexes);
  }
  return copied;
}

function copyColVindexes(original) {
  var copied = [];
  for (var key in original) {
    var colVindex = {};
    colVindex.Col = original[key].Col;
    colVindex.Name = original[key].Name;
    copied.push(colVindex);
    }
  return copied;
}


function copyUnshardedTables(original) {
  var copied = {};
  for (var key in original) {
    copied[key] = {};
  }
  return copied;
}
