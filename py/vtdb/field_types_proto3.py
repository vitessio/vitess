# Copyright 2015 Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This module defines the conversion functions from proto3 to python.
"""

from decimal import Decimal

from vtproto import query_pb2

from vtdb import times

conversions = {
    query_pb2.INT8: int,
    query_pb2.UINT8: int,
    query_pb2.INT16: int,
    query_pb2.UINT16: int,
    query_pb2.INT24: int,
    query_pb2.UINT24: int,
    query_pb2.INT32: int,
    query_pb2.UINT32: int,
    query_pb2.INT64: int,
    query_pb2.UINT64: long,
    query_pb2.FLOAT32: float,
    query_pb2.FLOAT64: float,
    query_pb2.TIMESTAMP: times.DateTimeOrNone,
    query_pb2.DATE: times.DateOrNone,
    query_pb2.TIME: times.TimeDeltaOrNone,
    query_pb2.DATETIME: times.DateTimeOrNone,
    query_pb2.YEAR: int,
    query_pb2.DECIMAL: Decimal,
    # query_pb2.TEXT: no conversion
    # query_pb2.BLOB: no conversion
    # query_pb2.VARCHAR: no conversion
    # query_pb2.VARBINARY: no conversion
    # query_pb2.CHAR: no conversion
    # query_pb2.BINARY: no conversion
    # query_pb2.BIT: no conversion
    # query_pb2.ENUM: no conversion
    # query_pb2.SET: no conversion
    # query_pb2.TUPLE: no conversion
}
