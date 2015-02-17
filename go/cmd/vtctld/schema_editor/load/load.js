/**
 * Copyright 2015, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function LoadController($scope, $http, curSchema) {
  init();

  function init() {
    $scope.loader = {};
  }

  function setLoaded(loaded) {
    $scope.loadedJSON = angular.toJson(loaded, true);
    curSchema.init(loaded.Keyspaces);
    $scope.clearLoaderError();
  }

  $scope.loadFromTopo = function() {
    $http.get("/vschema").success(function(data, status, headers, config) {
      try {
        var parser = new DOMParser();
        var xmlDoc = parser.parseFromString(data, "text/xml");
        var err = xmlDoc.getElementById("err");
        if (err) {
          $scope.loader.err = err.innerHTML;
          return;
        }
        var initialJSON = xmlDoc.getElementById("vschema").innerHTML;
        setLoaded(angular.fromJson(initialJSON));
      } catch (err) {
        $scope.loader.err = err.message;
      }
    }).error(function(data, status, headers, config) {
      $scope.loader.err = data;
    });
  };

  $scope.loadTestData = function() {
    var testData = {
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
                }
            },
            "Classes": {
                "user": {
                  "ColVindexes": [
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
                  ]
                },
                "user_extra": {
                  "ColVindexes": [
                      {
                          "Col": "user_id",
                          "Name": "user_index"
                      }, {
                          "Col": "id",
                          "Name": "user_extra_index"
                      }
                  ]
                },
                "music": {
                  "ColVindexes": [
                      {
                          "Col": "user_id",
                          "Name": "name_user_map"
                      }, {
                          "Col": "id",
                          "Name": "user_index"
                      }
                  ]
                },
                "music_extra": {
                  "ColVindexes": [
                      {
                          "Col": "user_id",
                          "Name": "music_user_map"
                      }, {
                          "Col": "music_id",
                          "Name": "user_index1"
                      }
                  ]
                }
            },
            "Tables": {
                "user": "aa",
                "user_extra": "user_extra",
                "music": "music",
                "music_extra": "music_extra",
                "very_very_long_name": "music_extra"
            }
        },
        "main": {
          "Tables": {
              "main1": "aa",
              "main2": "",
              "music_extra": ""
          }
        }
    };
    setLoaded({
      "Keyspaces": testData
    });
  };

  $scope.clearLoaderError = function() {
    $scope.loader.err = "";
  };
}