/**
 * Copyright 2014, Google Inc. All rights reserved. Use of this source code is
 * governed by a BSD-style license that can be found in the LICENSE file.
 */
'use strict';

function vindexInfo() {
  var info = {};
  info.Types = {
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
  info.TypeNames = Object.keys(info.Types);
  return info;
}