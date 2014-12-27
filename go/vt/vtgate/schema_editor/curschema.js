/**
 * Copyright 2014, Google Inc. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
'use strict';

function curSchema(){
  var data = {};
  data.vschema = {
          "Keyspaces": {
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
          }
        };
        return data;
}