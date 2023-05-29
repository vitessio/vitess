/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/google/go-cmp/cmp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	validSQL = []struct {
		input                string
		output               string
		partialDDL           bool
		ignoreNormalizerTest bool
	}{{
		input:  "create table x(location GEOMETRYCOLLECTION DEFAULT (POINT(7.0, 3.0)))",
		output: "create table x (\n\tlocation GEOMETRYCOLLECTION default (point(7.0, 3.0))\n)",
	}, {
		input:  "create table t (id int primary key, dt datetime DEFAULT (CURRENT_TIMESTAMP))",
		output: "create table t (\n\tid int primary key,\n\tdt datetime default current_timestamp()\n)",
	}, {
		input:  "create table t (id int primary key, dt datetime DEFAULT now())",
		output: "create table t (\n\tid int primary key,\n\tdt datetime default now()\n)",
	}, {
		input:  "create table t (id int primary key, dt datetime DEFAULT (now()))",
		output: "create table t (\n\tid int primary key,\n\tdt datetime default now()\n)",
	}, {
		input:  "create table x (e enum('red','yellow') null collate 'utf8_bin')",
		output: "create table x (\n\te enum('red', 'yellow') collate 'utf8_bin' null\n)",
	}, {
		input:  "create table 3t2 (c1 bigint not null, c2 text, primary key(c1))",
		output: "create table `3t2` (\n\tc1 bigint not null,\n\tc2 text,\n\tprimary key (c1)\n)",
	}, {
		input:  "select 1 from t1 where exists (select 1) = TRUE",
		output: "select 1 from t1 where exists (select 1 from dual) = true",
	}, {
		input:  "select 1 from t1 where exists (select 1) = FALSE",
		output: "select 1 from t1 where exists (select 1 from dual) = false",
	}, {
		input:  "select 1 from t1 where exists (select 1) = 1",
		output: "select 1 from t1 where exists (select 1 from dual) = 1",
	}, {
		input:  "create table x(location GEOMETRY DEFAULT (POINT(7.0, 3.0)))",
		output: "create table x (\n\tlocation GEOMETRY default (point(7.0, 3.0))\n)",
	}, {
		input:  "select 1",
		output: "select 1 from dual",
	}, {
		input:  "SELECT EXTRACT(YEAR FROM '2019-07-02')",
		output: "select extract(year from '2019-07-02') from dual",
	}, {
		input:  "SELECT EXTRACT(YEAR_MONTH FROM '2019-07-02 01:02:03')",
		output: "select extract(year_month from '2019-07-02 01:02:03') from dual",
	}, {
		input:  "select extract(year from \"21-10-22 12:00:00\")",
		output: "select extract(year from '21-10-22 12:00:00') from dual",
	}, {
		input:  "SELECT EXTRACT(DAY_MINUTE FROM '2019-07-02 01:02:03')",
		output: "select extract(day_minute from '2019-07-02 01:02:03') from dual",
	}, {
		input:  "SELECT EXTRACT(MICROSECOND FROM '2003-01-02 10:30:00.000123')",
		output: "select extract(microsecond from '2003-01-02 10:30:00.000123') from dual",
	}, {
		input:  "CREATE TABLE t2 (b BLOB DEFAULT 'abc')",
		output: "create table t2 (\n\tb BLOB default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b blob DEFAULT 'abc')",
		output: "create table t2 (\n\tb blob default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b BLOB DEFAULT ('abc'))",
		output: "create table t2 (\n\tb BLOB default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b TINYBLOB DEFAULT 'abc')",
		output: "create table t2 (\n\tb TINYBLOB default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b TINYBLOB DEFAULT ('abc'))",
		output: "create table t2 (\n\tb TINYBLOB default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b MEDIUMBLOB DEFAULT 'abc')",
		output: "create table t2 (\n\tb MEDIUMBLOB default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b MEDIUMBLOB DEFAULT ('abc'))",
		output: "create table t2 (\n\tb MEDIUMBLOB default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b LONGBLOB DEFAULT 'abc')",
		output: "create table t2 (\n\tb LONGBLOB default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b LONGBLOB DEFAULT ('abc'))",
		output: "create table t2 (\n\tb LONGBLOB default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b TEXT DEFAULT 'abc')",
		output: "create table t2 (\n\tb TEXT default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b TEXT DEFAULT ('abc'))",
		output: "create table t2 (\n\tb TEXT default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b TINYTEXT DEFAULT 'abc')",
		output: "create table t2 (\n\tb TINYTEXT default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b TINYTEXT DEFAULT ('abc'))",
		output: "create table t2 (\n\tb TINYTEXT default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b MEDIUMTEXT DEFAULT 'abc')",
		output: "create table t2 (\n\tb MEDIUMTEXT default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b MEDIUMTEXT DEFAULT ('abc'))",
		output: "create table t2 (\n\tb MEDIUMTEXT default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b LONGTEXT DEFAULT 'abc')",
		output: "create table t2 (\n\tb LONGTEXT default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b LONGTEXT DEFAULT ('abc'))",
		output: "create table t2 (\n\tb LONGTEXT default ('abc')\n)",
	}, {
		input:  "CREATE TABLE t2 (b JSON DEFAULT null)",
		output: "create table t2 (\n\tb JSON default null\n)",
	}, {
		input:  "CREATE TABLE t2 (b JSON DEFAULT (null))",
		output: "create table t2 (\n\tb JSON default null\n)",
	}, {
		input:  "CREATE TABLE t2 (b JSON DEFAULT '{name:abc}')",
		output: "create table t2 (\n\tb JSON default ('{name:abc}')\n)",
	}, {
		input:  "CREATE TABLE t2 (b JSON DEFAULT ('{name:abc}'))",
		output: "create table t2 (\n\tb JSON default ('{name:abc}')\n)",
	}, {
		input:  "create table x(location POINT DEFAULT 7.0)",
		output: "create table x (\n\tlocation POINT default (7.0)\n)",
	}, {
		input:  "create table x(location POINT DEFAULT (7.0))",
		output: "create table x (\n\tlocation POINT default (7.0)\n)",
	}, {
		input:  "create table x(location LINESTRING DEFAULT (POINT(7.0, 3.0)))",
		output: "create table x (\n\tlocation LINESTRING default (point(7.0, 3.0))\n)",
	}, {
		input:  "select linestring(pt1, pt2) from geom",
		output: "select linestring(pt1, pt2) from geom",
	}, {
		input:  "create table x(location POLYGON DEFAULT (POINT(7.0, 3.0)))",
		output: "create table x (\n\tlocation POLYGON default (point(7.0, 3.0))\n)",
	}, {
		input:  "create table x(location MULTIPOINT DEFAULT (POINT(7.0, 3.0)))",
		output: "create table x (\n\tlocation MULTIPOINT default (point(7.0, 3.0))\n)",
	}, {
		input:  "create table x(location MULTILINESTRING DEFAULT (POINT(7.0, 3.0)))",
		output: "create table x (\n\tlocation MULTILINESTRING default (point(7.0, 3.0))\n)",
	}, {
		input:  "create table x(location MULTIPOLYGON DEFAULT (POINT(7.0, 3.0)))",
		output: "create table x (\n\tlocation MULTIPOLYGON default (point(7.0, 3.0))\n)",
	}, {
		input:  "create table x(location GEOMETRYCOLLECTION DEFAULT (POINT(7.0, 3.0)))",
		output: "create table x (\n\tlocation GEOMETRYCOLLECTION default (point(7.0, 3.0))\n)",
	}, {
		input:  "create table x(location GEOMETRYCOLLECTION DEFAULT (LINESTRING(POINT(4, 5), POINT(4.6, 7.9), POINT(4.6, 7.9))))",
		output: "create table x (\n\tlocation GEOMETRYCOLLECTION default (linestring(point(4, 5), point(4.6, 7.9), point(4.6, 7.9)))\n)",
	}, {
		input:  "create table x(location GEOMETRYCOLLECTION DEFAULT (LINESTRING(POINT(7.0, 3.0))))",
		output: "create table x (\n\tlocation GEOMETRYCOLLECTION default (linestring(point(7.0, 3.0)))\n)",
	}, {
		input:  "create table x(location GEOMETRYCOLLECTION DEFAULT (POLYGON(LINESTRING(POINT(4, 5), POINT(4.6, 7.9), POINT(4.6, 7.9)))))",
		output: "create table x (\n\tlocation GEOMETRYCOLLECTION default (polygon(linestring(point(4, 5), point(4.6, 7.9), point(4.6, 7.9))))\n)",
	}, {
		input:  "select ST_ASTEXT(POLYGON(linestrings)) from linestringTable",
		output: "select st_astext(polygon(linestrings)) from linestringTable",
	}, {
		input:  "create table x(location GEOMETRYCOLLECTION DEFAULT (MULTIPOINT(POINT(4, 5), POINT(4.6, 7.9), POINT(4.6, 7.9))))",
		output: "create table x (\n\tlocation GEOMETRYCOLLECTION default (multipoint(point(4, 5), point(4.6, 7.9), point(4.6, 7.9)))\n)",
	}, {
		input:  "select ST_ASTEXT(MULTIPOINT(points)) from pointsTable",
		output: "select st_astext(multipoint(points)) from pointsTable",
	}, {
		input:  "create table x(location GEOMETRYCOLLECTION DEFAULT (MULTILINESTRING(LINESTRING(POINT(8,9), POINT(8,9)))))",
		output: "create table x (\n\tlocation GEOMETRYCOLLECTION default (multilinestring(linestring(point(8, 9), point(8, 9))))\n)",
	}, {
		input:  "select ST_ASTEXT(MULTILINESTRING(linestrings)) from linestringsTable",
		output: "select st_astext(multilinestring(linestrings)) from linestringsTable",
	}, {
		input:  "create table x(location GEOMETRYCOLLECTION DEFAULT (MULTIPOLYGON(POINT(7.0, 3.0), POINT(7.0, 3.0))))",
		output: "create table x (\n\tlocation GEOMETRYCOLLECTION default (multipolygon(point(7.0, 3.0), point(7.0, 3.0)))\n)",
	}, {
		input:  "select ST_ASTEXT(MULTIPOLYGON(polygons)) from polygonTable",
		output: "select st_astext(multipolygon(polygons)) from polygonTable",
	}, {
		input:  "SELECT ST_AsText(ST_GeomCollFromText('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))'))",
		output: "select st_astext(st_geometrycollectionfromtext('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomCollFromText('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326))",
		output: "select st_astext(st_geometrycollectionfromtext('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomCollFromText('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_geometrycollectionfromtext('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomFromText('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))'))",
		output: "select st_astext(st_geometryfromtext('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomFromText('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326))",
		output: "select st_astext(st_geometryfromtext('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomFromText('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_geometryfromtext('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultilinestringFromText('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))'))",
		output: "select st_astext(st_multilinestringfromtext('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultilinestringFromText('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326))",
		output: "select st_astext(st_multilinestringfromtext('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultilinestringFromText('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_multilinestringfromtext('MULTILINESTRING((10 10, 11 11), (9 9, 10 10))', 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_LinestringFromText('LINESTRING((10 10, 11 11))'))",
		output: "select st_astext(st_linestringfromtext('LINESTRING((10 10, 11 11))')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_LinestringFromText('LINESTRING((10 10, 11 11))', 4326))",
		output: "select st_astext(st_linestringfromtext('LINESTRING((10 10, 11 11))', 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_LinestringFromText('LINESTRING((10 10, 11 11))', 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_linestringfromtext('LINESTRING((10 10, 11 11))', 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PointFromText('POINT(10 10)'))",
		output: "select st_astext(st_pointfromtext('POINT(10 10)')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PointFromText('POINT(10 10)', 4326))",
		output: "select st_astext(st_pointfromtext('POINT(10 10)', 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPointFromText('MULTIPOINT((10 10, 11 11))'))",
		output: "select st_astext(st_multipointfromtext('MULTIPOINT((10 10, 11 11))')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPointFromText('MULTIPOINT((10 10, 11 11))', 4326))",
		output: "select st_astext(st_multipointfromtext('MULTIPOINT((10 10, 11 11))', 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPointFromText('MULTIPOINT((10 10, 11 11))', 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_multipointfromtext('MULTIPOINT((10 10, 11 11))', 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPolygonFromText(@g))",
		output: "select st_astext(st_multipolygonfromtext(@g)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPolygonFromText(@g, 4326))",
		output: "select st_astext(st_multipolygonfromtext(@g, 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPolygonFromText(@g, 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_multipolygonfromtext(@g, 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PolygonFromText(@g))",
		output: "select st_astext(st_polygonfromtext(@g)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PolygonFromText(@g, 4326))",
		output: "select st_astext(st_polygonfromtext(@g, 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PolygonFromText(@g, 4326, 'axis-order=long-lat'))",
		output: "select st_astext(st_polygonfromtext(@g, 4326, 'axis-order=long-lat')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomCollFromWKB(0x010100000000000000000022400000000000002240))",
		output: "select st_astext(st_geometrycollectionfromwkb(0x010100000000000000000022400000000000002240)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomCollFromWKB(g, 4326))",
		output: "select st_astext(st_geometrycollectionfromwkb(g, 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomCollFromText(g, 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_geometrycollectionfromtext(g, 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomFromWKB(g))",
		output: "select st_astext(st_geometryfromwkb(g)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomFromWKB(g, 4326))",
		output: "select st_astext(st_geometryfromwkb(g, 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomFromWKB(g, 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_geometryfromwkb(g, 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultilinestringFromWKB(g))",
		output: "select st_astext(st_multilinestringfromwkb(g)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultilinestringFromWKB(g, 4326))",
		output: "select st_astext(st_multilinestringfromwkb(g, 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultilinestringFromWKB(0x01050000000200000001020000000200000000000000000024400000000000002440000000000000264000000000000026400102000000020000000000000000002240000000000000224000000000000024400000000000002440, 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_multilinestringfromwkb(0x01050000000200000001020000000200000000000000000024400000000000002440000000000000264000000000000026400102000000020000000000000000002240000000000000224000000000000024400000000000002440, 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_LinestringFromWKB(g))",
		output: "select st_astext(st_linestringfromwkb(g)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_LinestringFromWKB(g, 4326))",
		output: "select st_astext(st_linestringfromwkb(g, 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_LinestringFromWKB(g, 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_linestringfromwkb(g, 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PointFromWKB(mp))",
		output: "select st_astext(st_pointfromwkb(mp)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PointFromWKB(mp, 4326))",
		output: "select st_astext(st_pointfromwkb(mp, 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPointFromWKB(mp))",
		output: "select st_astext(st_multipointfromwkb(mp)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPointFromWKB(mp, 4326))",
		output: "select st_astext(st_multipointfromwkb(mp, 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPointFromWKB(mp, 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_multipointfromwkb(mp, 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPolygonFromText(g))",
		output: "select st_astext(st_multipolygonfromtext(g)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPolygonFromText(g, 4326))",
		output: "select st_astext(st_multipolygonfromtext(g, 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_MultiPolygonFromText(g, 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_multipolygonfromtext(g, 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PolygonFromText(g))",
		output: "select st_astext(st_polygonfromtext(g)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PolygonFromText(g, 4326))",
		output: "select st_astext(st_polygonfromtext(g, 4326)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PolygonFromText(g, 4326, 'axis-order=long-lat'), 'axis-order=long-lat')",
		output: "select st_astext(st_polygonfromtext(g, 4326, 'axis-order=long-lat'), 'axis-order=long-lat') from dual",
	}, {
		input:  "SELECT ST_AsWKT(ST_GeomCollFromText(g, 4326, 'axis-order=lat-long'))",
		output: "select st_astext(st_geometrycollectionfromtext(g, 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_AsBinary(ST_PolygonFromText(g, 4326, 'axis-order=long-lat'), 'axis-order=long-lat')",
		output: "select st_asbinary(st_polygonfromtext(g, 4326, 'axis-order=long-lat'), 'axis-order=long-lat') from dual",
	}, {
		input:  "SELECT ST_AsWKB(ST_GeomCollFromText(g, 4326, 'axis-order=lat-long'))",
		output: "select st_asbinary(st_geometrycollectionfromtext(g, 4326, 'axis-order=lat-long')) from dual",
	}, {
		input:  "SELECT ST_Dimension(ST_GeomFromText('LineString(1 1,2 2)'))",
		output: "select st_dimension(st_geometryfromtext('LineString(1 1,2 2)')) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_Envelope(ST_GeomFromText('LineString(1 1,2 2)')))",
		output: "select st_astext(st_envelope(st_geometryfromtext('LineString(1 1,2 2)'))) from dual",
	}, {
		input:  "SELECT ST_IsSimple(ST_GeomFromText('POINT(1 1)'))",
		output: "select st_issimple(st_geometryfromtext('POINT(1 1)')) from dual",
	}, {
		input:  "SELECT ST_GeometryType(ST_GeomFromText('POINT(1 1)'))",
		output: "select st_geometrytype(st_geometryfromtext('POINT(1 1)')) from dual",
	}, {
		input:  "SELECT ST_IsEmpty(ST_GeomFromText('POINT(1 1)'))",
		output: "select st_isempty(st_geometryfromtext('POINT(1 1)')) from dual",
	}, {
		input:  "SELECT ST_Latitude(@pt);",
		output: "select st_latitude(@pt) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_Latitude(@pt, 10));",
		output: "select st_astext(st_latitude(@pt, 10)) from dual",
	}, {
		input:  "SELECT ST_Longitude(@pt);",
		output: "select st_longitude(@pt) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_Longitude(@pt, 10));",
		output: "select st_astext(st_longitude(@pt, 10)) from dual",
	}, {
		input:  "SELECT ST_X(@pt);",
		output: "select st_x(@pt) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_X(@pt, 10));",
		output: "select st_astext(st_x(@pt, 10)) from dual",
	}, {
		input:  "SELECT ST_Y(@pt);",
		output: "select st_y(@pt) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_Y(@pt, 10));",
		output: "select st_astext(st_y(@pt, 10)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_EndPoint(ST_GeomFromText(@ls)));",
		output: "select st_astext(st_endpoint(st_geometryfromtext(@ls))) from dual",
	}, {
		input:  "SELECT ST_IsClosed(ST_GeomFromText(@ls1));",
		output: "select st_isclosed(st_geometryfromtext(@ls1)) from dual",
	}, {
		input:  "SELECT IsClosed(ST_GeomFromText(@ls1));",
		output: "select st_isclosed(st_geometryfromtext(@ls1)) from dual",
	}, {
		input:  "SELECT ST_Length(@ls);",
		output: "select st_length(@ls) from dual",
	}, {
		input:  "SELECT ST_Length(@ls, 'metre');",
		output: "select st_length(@ls, 'metre') from dual",
	}, {
		input:  "SELECT GLength(@ls);",
		output: "select st_length(@ls) from dual",
	}, {
		input:  "SELECT GLength(@ls, 'metre');",
		output: "select st_length(@ls, 'metre') from dual",
	}, {
		input:  "SELECT ST_NumPoints(ST_GeomFromText(@ls));",
		output: "select st_numpoints(st_geometryfromtext(@ls)) from dual",
	}, {
		input:  "SELECT Numpoints(ST_GeomFromText(@ls));",
		output: "select st_numpoints(st_geometryfromtext(@ls)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PointN(ST_GeomFromText(@ls),2));",
		output: "select st_astext(st_pointn(st_geometryfromtext(@ls), 2)) from dual",
	}, {
		input:  "SELECT ST_AsText(PointN(ST_GeomFromText(@ls),2));",
		output: "select st_astext(st_pointn(st_geometryfromtext(@ls), 2)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_StartPoint(ST_GeomFromText(@ls)));",
		output: "select st_astext(st_startpoint(st_geometryfromtext(@ls))) from dual",
	}, {
		input:  "SELECT ST_AsText(StartPoint(ST_GeomFromText(@ls)));",
		output: "select st_astext(st_startpoint(st_geometryfromtext(@ls))) from dual",
	}, {
		input:  "SELECT ST_Area(ST_GeomFromText(@mpoly));",
		output: "select st_area(st_geometryfromtext(@mpoly)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeometryN(ST_GeomFromText(@gc),1));",
		output: "select st_astext(st_geometryn(st_geometryfromtext(@gc), 1)) from dual",
	}, {
		input:  "SELECT ST_NumGeometries(ST_GeomFromText(@gc));",
		output: "select st_numgeometries(st_geometryfromtext(@gc)) from dual",
	}, {
		input:  "SELECT ST_GeometryType(@poly),ST_AsText(ST_Centroid(@poly));",
		output: "select st_geometrytype(@poly), st_astext(st_centroid(@poly)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_ExteriorRing(ST_GeomFromText(@poly)));",
		output: "select st_astext(st_exteriorring(st_geometryfromtext(@poly))) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_InteriorRingN(ST_GeomFromText(@poly),1));",
		output: "select st_astext(st_interiorringN(st_geometryfromtext(@poly), 1)) from dual",
	}, {
		input:  "SELECT ST_NumInteriorRings(ST_GeomFromText(@poly));",
		output: "select st_numinteriorrings(st_geometryfromtext(@poly)) from dual",
	}, {
		input:  "SELECT ST_NumInteriorRing(ST_GeomFromText(@poly));",
		output: "select st_numinteriorrings(st_geometryfromtext(@poly)) from dual",
	}, {
		input:  "SELECT ST_GeoHash(180,0,10), ST_GeoHash(-180,-90,15);",
		output: "select st_geohash(180, 0, 10), st_geohash(-180, -90, 15) from dual",
	}, {
		input:  "SELECT ST_GeoHash(@p,10);",
		output: "select st_geohash(@p, 10) from dual",
	}, {
		input:  "SELECT ST_LatFromGeoHash(ST_GeoHash(45,-20,10));",
		output: "select st_latfromgeohash(st_geohash(45, -20, 10)) from dual",
	}, {
		input:  "SELECT ST_LongFromGeoHash(ST_GeoHash(45,-20,10));",
		output: "select st_longfromgeohash(st_geohash(45, -20, 10)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_PointFromGeoHash(@gh,0));",
		output: "select st_astext(st_pointfromgeohash(@gh, 0)) from dual",
	}, {
		input:  "SELECT ST_AsGeoJSON(ST_GeomFromText('POINT(11.11111 12.22222)'));",
		output: "select st_asgeojson(st_geometryfromtext('POINT(11.11111 12.22222)')) from dual",
	}, {
		input:  "SELECT ST_AsGeoJSON(ST_GeomFromText('POINT(11.11111 12.22222)'),2);",
		output: "select st_asgeojson(st_geometryfromtext('POINT(11.11111 12.22222)'), 2) from dual",
	}, {
		input:  "SELECT ST_AsGeoJSON(ST_GeomFromText('POINT(11.11111 12.22222)'),2,0);",
		output: "select st_asgeojson(st_geometryfromtext('POINT(11.11111 12.22222)'), 2, 0) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_GeomFromGeoJSON(@json));",
		output: "select st_astext(st_geomfromgeojson(@`json`)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_SRID(ST_GeomFromGeoJSON(@json, 0),0));",
		output: "select st_astext(ST_SRID(st_geomfromgeojson(@`json`, 0), 0)) from dual",
	}, {
		input:  "SELECT ST_AsText(ST_SRID(ST_GeomFromGeoJSON(@json),1,4326));",
		output: "select st_astext(ST_SRID(st_geomfromgeojson(@`json`), 1, 4326)) from dual",
	}, {
		input:  "WITH RECURSIVE  odd_num_cte (id, n) AS (SELECT 1, 1 union all SELECT id+1, n+2 from odd_num_cte where id < 5) SELECT * FROM odd_num_cte",
		output: "with recursive odd_num_cte(id, n) as (select 1, 1 from dual union all select id + 1, n + 2 from odd_num_cte where id < 5) select * from odd_num_cte",
	}, {
		input:  "WITH topsales2003 AS (SELECT salesRepEmployeeNumber employeeNumber, SUM(quantityOrdered * priceEach) sales FROM orders INNER JOIN orderdetails USING (orderNumber) INNER JOIN customers USING (customerNumber) WHERE YEAR(shippedDate) = 2003 AND status = 'Shipped' GROUP BY salesRepEmployeeNumber ORDER BY sales DESC LIMIT 5)SELECT employeeNumber, firstName, lastName, sales FROM employees JOIN topsales2003 USING (employeeNumber)",
		output: "with topsales2003 as (select salesRepEmployeeNumber as employeeNumber, sum(quantityOrdered * priceEach) as sales from orders join orderdetails using (orderNumber) join customers using (customerNumber) where YEAR(shippedDate) = 2003 and `status` = 'Shipped' group by salesRepEmployeeNumber order by sales desc limit 5) select employeeNumber, firstName, lastName, sales from employees join topsales2003 using (employeeNumber)",
	}, {
		input: "select 1 from t",
	}, {
		input:  "select * from (select 1) as x(user)",
		output: "select * from (select 1 from dual) as x(`user`)",
	}, {
		input:  "select user from (select id from users ) as x(user)",
		output: "select `user` from (select id from users) as x(`user`)",
	}, {
		input: "select n, d from something",
	}, {
		input: "insert into sys_message_assign(message_id, assign_user_id, read_state, id, is_delete, create_time, update_time, remark) values (N'3477028275831808', N'4104487936', N'1', N'0', N'0', '2021-09-22 14:24:17.922', '2021-09-22 14:24:17.922', null), (N'3477028275831808', N'3454139190608923', N'1', N'0', N'0', '2021-09-22 14:24:17.922', '2021-09-22 14:24:17.922', null)",
		/*We need to ignore this test because, after the normalizer, we change the produced NChar
		string into an introducer expression, so the vttablet will never see a NChar string */
		ignoreNormalizerTest: true,
	}, {
		input:  "select name, numbers from (select * from users) as x(name, numbers)",
		output: "select `name`, numbers from (select * from users) as x(`name`, numbers)",
	}, {
		input:  "select 0b010, 0b0111, b'0111', b'011'",
		output: "select B'010', B'0111', B'0111', B'011' from dual",
	}, {
		input:  "select 0x010, 0x0111, x'0111'",
		output: "select 0x010, 0x0111, X'0111' from dual",
	}, {
		input:  "select date'2022-10-03'",
		output: "select date'2022-10-03' from dual",
	}, {
		input:  "select date, time, timestamp from t",
		output: "select `date`, `time`, `timestamp` from t",
	}, {
		input:  "select time'12:34:56'",
		output: "select time'12:34:56' from dual",
	}, {
		input:  "select timestamp'2012-12-31 11:30:45'",
		output: "select timestamp'2012-12-31 11:30:45' from dual",
	}, {
		input:  "select * from information_schema.columns",
		output: "select * from information_schema.`columns`",
	}, {
		input:  "select * from information_schema.processlist",
		output: "select * from information_schema.`processlist`",
	}, {
		input: "select .1 from t",
	}, {
		input: "select 1.2e1 from t",
	}, {
		input: "select 1.2e+1 from t",
	}, {
		input: "select 1.2e-1 from t",
	}, {
		input: "select 08.3 from t",
	}, {
		input: "select -1 from t where b = -2",
	}, {
		input:  "select - -1 from t",
		output: "select - -1 from t",
	}, {
		input: "select a from t",
	}, {
		input: "select $ from t",
	}, {
		// shift/reduce conflict on CHARSET, should throw an error on shifting which will be ignored as it is a DDL
		input:      "alter database charset = 'utf16';",
		output:     "alter database",
		partialDDL: true,
	}, {
		input:  "alter database charset charset = 'utf16'",
		output: "alter database `charset` character set 'utf16'",
	}, {
		input:  "create table t(id int unique)",
		output: "create table t (\n\tid int unique\n)",
	}, {
		input:  "create table t(id int key)",
		output: "create table t (\n\tid int key\n)",
	}, {
		input:  "create table t(id int unique key)",
		output: "create table t (\n\tid int unique key\n)",
	}, {
		input: "select a.b as a$b from $test$",
	}, {
		input:  "select 1 from t // aa\n",
		output: "select 1 from t",
	}, {
		input:  "select 1 from t -- aa\n",
		output: "select 1 from t",
	}, {
		input:  "select 1 from t # aa\n",
		output: "select 1 from t",
	}, {
		input:  "select 1 -- aa\nfrom t",
		output: "select 1 from t",
	}, {
		input:  "select 1 #aa\nfrom t",
		output: "select 1 from t",
	}, {
		input: "select /* simplest */ 1 from t",
	}, {
		input: "select /* double star **/ 1 from t",
	}, {
		input: "select /* double */ /* comment */ 1 from t",
	}, {
		input: "select /* back-quote keyword */ `By` from t",
	}, {
		input: "select /* back-quote num */ `2a` from t",
	}, {
		input: "select /* back-quote . */ `a.b` from t",
	}, {
		input: "select /* back-quote back-quote */ `a``b` from t",
	}, {
		input:  "select /* back-quote unnecessary */ 1 from `t`",
		output: "select /* back-quote unnecessary */ 1 from t",
	}, {
		input:  "select /* back-quote idnum */ 1 from `a1`",
		output: "select /* back-quote idnum */ 1 from a1",
	}, {
		input: "select /* @ */ @@a from b",
	}, {
		input: "select /* \\0 */ '\\0' from a",
	}, {
		input:  "select 1 /* drop this comment */ from t",
		output: "select 1 from t",
	}, {
		input: "select /* union */ 1 from t union select 1 from t",
	}, {
		input: "select /* double union */ 1 from t union select 1 from t union select 1 from t",
	}, {
		input: "select /* union all */ 1 from t union all select 1 from t",
	}, {
		input:  "select /* union distinct */ 1 from t union distinct select 1 from t",
		output: "select /* union distinct */ 1 from t union select 1 from t",
	}, {
		input:  "(select /* union parenthesized select */ 1 from t order by a) union select 1 from t",
		output: "(select /* union parenthesized select */ 1 from t order by a asc) union select 1 from t",
	}, {
		input:  "select /* union parenthesized select 2 */ 1 from t union (select 1 from t)",
		output: "select /* union parenthesized select 2 */ 1 from t union select 1 from t",
	}, {
		input:  "select /* union order by */ 1 from t union select 1 from t order by a",
		output: "select /* union order by */ 1 from t union select 1 from t order by a asc",
	}, {
		input:  "select /* union order by limit lock */ 1 from t union select 1 from t order by a limit 1 for update",
		output: "select /* union order by limit lock */ 1 from t union select 1 from t order by a asc limit 1 for update",
	}, {
		input:  "(select id, a from t order by id limit 1) union (select id, b as a from s order by id limit 1) order by a limit 1",
		output: "(select id, a from t order by id asc limit 1) union (select id, b as a from s order by id asc limit 1) order by a asc limit 1",
	}, {
		input:  "select a from (select 1 as a from tbl1 union select 2 from tbl2) as t",
		output: "select a from (select 1 as a from tbl1 union select 2 from tbl2) as t",
	}, {
		input: "select * from t1 join (select * from t2 union select * from t3) as t",
	}, {
		// Ensure this doesn't generate: ""select * from t1 join t2 on a = b join t3 on a = b".
		input: "select * from t1 join t2 on a = b join t3",
	}, {
		input:  "select * from t1 where col in (select 1 from dual union select 2 from dual)",
		output: "select * from t1 where col in (select 1 from dual union select 2 from dual)",
	}, {
		input: "select * from t1 where exists (select a from t2 union select b from t3)",
	}, {
		input:  "select 1 from dual union select 2 from dual union all select 3 from dual union select 4 from dual union all select 5 from dual",
		output: "select 1 from dual union select 2 from dual union all select 3 from dual union select 4 from dual union all select 5 from dual",
	}, {
		input:  "(select 1 from dual) order by 1 asc limit 2",
		output: "select 1 from dual order by 1 asc limit 2",
	}, {
		input:  "(select 1 from dual order by 1 desc) order by 1 asc limit 2",
		output: "select 1 from dual order by 1 asc limit 2",
	}, {
		input:  "(select 1 from dual)",
		output: "select 1 from dual",
	}, {
		input:  "((select 1 from dual))",
		output: "select 1 from dual",
	}, {
		input: "select 1 from (select 1 from dual) as t",
	}, {
		input:  "select 1 from (select 1 from dual union select 2 from dual) as t",
		output: "select 1 from (select 1 from dual union select 2 from dual) as t",
	}, {
		input:  "select 1 from ((select 1 from dual) union select 2 from dual) as t",
		output: "select 1 from (select 1 from dual union select 2 from dual) as t",
	}, {
		input: "select /* distinct */ distinct 1 from t",
	}, {
		input: "select /* straight_join */ straight_join 1 from t",
	}, {
		input: "select /* for update */ 1 from t for update",
	}, {
		input: "select /* lock in share mode */ 1 from t lock in share mode",
	}, {
		input: "select /* select list */ 1, 2 from t",
	}, {
		input: "select /* * */ * from t",
	}, {
		input: "select /* a.* */ a.* from t",
	}, {
		input: "select /* a.b.* */ a.b.* from t",
	}, {
		input:  "select /* column alias */ a b from t",
		output: "select /* column alias */ a as b from t",
	}, {
		input: "select /* column alias with as */ a as b from t",
	}, {
		input: "select /* keyword column alias */ a as `By` from t",
	}, {
		input:  "select /* column alias as string */ a as \"b\" from t",
		output: "select /* column alias as string */ a as b from t",
	}, {
		input:  "select /* column alias as string without as */ a \"b\" from t",
		output: "select /* column alias as string without as */ a as b from t",
	}, {
		input:  "select /* column alias with non_reserved keyword */ a as auto_increment from t",
		output: "select /* column alias with non_reserved keyword */ a as `auto_increment` from t",
	}, {
		input: "select /* a.* */ a.* from t",
	}, {
		input:  "select next value for t",
		output: "select next 1 values from t",
	}, {
		input:  "select next value from t",
		output: "select next 1 values from t",
	}, {
		input: "select next 10 values from t",
	}, {
		input: "select next :a values from t",
	}, {
		input: "select /* `By`.* */ `By`.* from t",
	}, {
		input: "select /* select with bool expr */ a = b from t",
	}, {
		input: "select /* case_when */ case when a = b then c end from t",
	}, {
		input: "select /* case_when_else */ case when a = b then c else d end from t",
	}, {
		input: "select /* case_when_when_else */ case when a = b then c when b = d then d else d end from t",
	}, {
		input: "select /* case */ case aa when a = b then c end from t",
	}, {
		input: "select /* parenthesis */ 1 from (t)",
	}, {
		input: "select /* parenthesis multi-table */ 1 from (t1, t2)",
	}, {
		input: "select /* table list */ 1 from t1, t2",
	}, {
		input: "select /* parenthessis in table list 1 */ 1 from (t1), t2",
	}, {
		input:  "SELECT * FROM t1 USE INDEX (i1) IGNORE INDEX FOR ORDER BY (i2) ORDER BY a",
		output: "select * from t1 use index (i1) ignore index for order by (i2) order by a asc",
	}, {
		input:  "SELECT * FROM t1 USE INDEX FOR JOIN (i1) FORCE INDEX FOR JOIN (i2)",
		output: "select * from t1 use index for join (i1) force index for join (i2)",
	}, {
		input:  "SELECT * FROM t1 USE INDEX FOR JOIN (i1) FORCE INDEX FOR JOIN (i2) IGNORE KEY FOR GROUP BY (i1, i2)",
		output: "select * from t1 use index for join (i1) force index for join (i2) ignore index for group by (i1, i2)",
	}, {
		input:  "SELECT * FROM t1 USE KEY (), t2 FORCE KEY (i2), t3 IGNORE INDEX FOR GROUP BY (i1, i2)",
		output: "select * from t1 use index (), t2 force index (i2), t3 ignore index for group by (i1, i2)",
	}, {
		input: "select /* parenthessis in table list 2 */ 1 from t1, (t2)",
	}, {
		input: "select /* use */ 1 from t1 use index (a) where b = 1",
	}, {
		input: "select /* use */ 1 from t1 use index () where b = 1",
	}, {
		input: "select /* keyword index */ 1 from t1 use index (`By`) where b = 1",
	}, {
		input: "select /* ignore */ 1 from t1 as t2 ignore index (a), t3 use index (b) where b = 1",
	}, {
		input: "select /* use */ 1 from t1 as t2 use index (a), t3 use index (b) where b = 1",
	}, {
		input: "select /* force */ 1 from t1 as t2 force index (a), t3 force index (b) where b = 1",
	}, {
		input:  "select /* table alias */ 1 from t t1",
		output: "select /* table alias */ 1 from t as t1",
	}, {
		input: "select /* table alias with as */ 1 from t as t1",
	}, {
		input:  "select /* string table alias */ 1 from t as 't1'",
		output: "select /* string table alias */ 1 from t as t1",
	}, {
		input:  "select /* string table alias without as */ 1 from t 't1'",
		output: "select /* string table alias without as */ 1 from t as t1",
	}, {
		input: "select /* keyword table alias */ 1 from t as `By`",
	}, {
		input: "select /* join */ 1 from t1 join t2",
	}, {
		input: "select /* join on */ 1 from t1 join t2 on a = b",
	}, {
		input: "select /* join on */ 1 from t1 join t2 using (a)",
	}, {
		input:  "select /* inner join */ 1 from t1 inner join t2",
		output: "select /* inner join */ 1 from t1 join t2",
	}, {
		input:  "select /* cross join */ 1 from t1 cross join t2",
		output: "select /* cross join */ 1 from t1 join t2",
	}, {
		input: "select /* straight_join */ 1 from t1 straight_join t2",
	}, {
		input: "select /* straight_join on */ 1 from t1 straight_join t2 on a = b",
	}, {
		input: "select /* left join */ 1 from t1 left join t2 on a = b",
	}, {
		input: "select /* left join */ 1 from t1 left join t2 using (a)",
	}, {
		input:  "select /* left outer join */ 1 from t1 left outer join t2 on a = b",
		output: "select /* left outer join */ 1 from t1 left join t2 on a = b",
	}, {
		input:  "select /* left outer join */ 1 from t1 left outer join t2 using (a)",
		output: "select /* left outer join */ 1 from t1 left join t2 using (a)",
	}, {
		input: "select /* right join */ 1 from t1 right join t2 on a = b",
	}, {
		input: "select /* right join */ 1 from t1 right join t2 using (a)",
	}, {
		input:  "select /* right outer join */ 1 from t1 right outer join t2 on a = b",
		output: "select /* right outer join */ 1 from t1 right join t2 on a = b",
	}, {
		input:  "select /* right outer join */ 1 from t1 right outer join t2 using (a)",
		output: "select /* right outer join */ 1 from t1 right join t2 using (a)",
	}, {
		input: "select /* natural join */ 1 from t1 natural join t2",
	}, {
		input: "select /* natural left join */ 1 from t1 natural left join t2",
	}, {
		input:  "select /* natural left outer join */ 1 from t1 natural left join t2",
		output: "select /* natural left outer join */ 1 from t1 natural left join t2",
	}, {
		input: "select /* natural right join */ 1 from t1 natural right join t2",
	}, {
		input:  "select /* natural right outer join */ 1 from t1 natural right join t2",
		output: "select /* natural right outer join */ 1 from t1 natural right join t2",
	}, {
		input: "select /* join on */ 1 from t1 join t2 on a = b",
	}, {
		input: "select /* join using */ 1 from t1 join t2 using (a)",
	}, {
		input: "select /* join using (a, b, c) */ 1 from t1 join t2 using (a, b, c)",
	}, {
		input: "select /* s.t */ 1 from s.t",
	}, {
		input: "select /* keyword schema & table name */ 1 from `By`.`bY`",
	}, {
		input: "select /* select in from */ 1 from (select 1 from t) as a",
	}, {
		input:  "select /* select in from with no as */ 1 from (select 1 from t) a",
		output: "select /* select in from with no as */ 1 from (select 1 from t) as a",
	}, {
		input: "select /* where */ 1 from t where a = b",
	}, {
		input: "select /* and */ 1 from t where a = b and a = c",
	}, {
		input:  "select /* && */ 1 from t where a = b && a = c",
		output: "select /* && */ 1 from t where a = b and a = c",
	}, {
		input: "select /* or */ 1 from t where a = b or a = c",
	}, {
		input:  "select /* || */ 1 from t where a = b || a = c",
		output: "select /* || */ 1 from t where a = b or a = c",
	}, {
		input: "select /* not */ 1 from t where not a = b",
	}, {
		input: "select /* ! */ 1 from t where a = !1",
	}, {
		input: "select /* bool is */ 1 from t where a = b is null",
	}, {
		input: "select /* bool is not */ 1 from t where a = b is not false",
	}, {
		input: "select /* true */ 1 from t where true",
	}, {
		input: "select /* false */ 1 from t where false",
	}, {
		input: "select /* false on left */ 1 from t where false = 0",
	}, {
		input: "select /* exists */ 1 from t where exists (select 1 from t)",
	}, {
		input:  "select /* (boolean) */ 1 from t where not (a = b)",
		output: "select /* (boolean) */ 1 from t where not a = b",
	}, {
		input: "select /* in value list */ 1 from t where a in (b, c)",
	}, {
		input: "select /* in select */ 1 from t where a in (select 1 from t)",
	}, {
		input: "select /* not in */ 1 from t where a not in (b, c)",
	}, {
		input: "select /* like */ 1 from t where a like b",
	}, {
		input: "select /* like escape */ 1 from t where a like b escape '!'",
	}, {
		input: "select /* not like */ 1 from t where a not like b",
	}, {
		input: "select /* not like escape */ 1 from t where a not like b escape '$'",
	}, {
		input: "select /* regexp */ 1 from t where a regexp b",
	}, {
		input: "select /* not regexp */ 1 from t where a not regexp b",
	}, {
		input:  "select /* rlike */ 1 from t where a rlike b",
		output: "select /* rlike */ 1 from t where a regexp b",
	}, {
		input:  "select /* not rlike */ 1 from t where a not rlike b",
		output: "select /* not rlike */ 1 from t where a not regexp b",
	}, {
		input: "select /* between */ 1 from t where a between b and c",
	}, {
		input: "select /* not between */ 1 from t where a not between b and c",
	}, {
		input: "select /* is null */ 1 from t where a is null",
	}, {
		input: "select /* is not null */ 1 from t where a is not null",
	}, {
		input: "select /* is true */ 1 from t where a is true",
	}, {
		input: "select /* is not true */ 1 from t where a is not true",
	}, {
		input: "select /* is false */ 1 from t where a is false",
	}, {
		input: "select /* is not false */ 1 from t where a is not false",
	}, {
		input: "select /* < */ 1 from t where a < b",
	}, {
		input: "select /* <= */ 1 from t where a <= b",
	}, {
		input: "select /* >= */ 1 from t where a >= b",
	}, {
		input: "select /* > */ 1 from t where a > b",
	}, {
		input: "select /* != */ 1 from t where a != b",
	}, {
		input:  "select /* <> */ 1 from t where a <> b",
		output: "select /* <> */ 1 from t where a != b",
	}, {
		input: "select /* <=> */ 1 from t where a <=> b",
	}, {
		input: "select /* != */ 1 from t where a != b",
	}, {
		input: "select /* single value expre list */ 1 from t where a in (b)",
	}, {
		input: "select /* select as a value expression */ 1 from t where a = (select a from t)",
	}, {
		input:  "select /* parenthesised value */ 1 from t where a = (b)",
		output: "select /* parenthesised value */ 1 from t where a = b",
	}, {
		input:  "select /* over-parenthesize */ ((1)) from t where ((a)) in (((1))) and ((a, b)) in ((((1, 1))), ((2, 2)))",
		output: "select /* over-parenthesize */ 1 from t where a in (1) and (a, b) in ((1, 1), (2, 2))",
	}, {
		input:  "select /* dot-parenthesize */ (a.b) from t where (b.c) = 2",
		output: "select /* dot-parenthesize */ a.b from t where b.c = 2",
	}, {
		input: "select /* & */ 1 from t where a = b & c",
	}, {
		input: "select /* & */ 1 from t where a = b & c",
	}, {
		input: "select /* | */ 1 from t where a = b | c",
	}, {
		input: "select /* ^ */ 1 from t where a = b ^ c",
	}, {
		input: "select /* + */ 1 from t where a = b + c",
	}, {
		input: "select /* - */ 1 from t where a = b - c",
	}, {
		input: "select /* * */ 1 from t where a = b * c",
	}, {
		input: "select /* / */ 1 from t where a = b / c",
	}, {
		input: "select /* % */ 1 from t where a = b % c",
	}, {
		input: "select /* div */ 1 from t where a = b div c",
	}, {
		input:  "select /* MOD */ 1 from t where a = b MOD c",
		output: "select /* MOD */ 1 from t where a = b % c",
	}, {
		input: "select /* << */ 1 from t where a = b << c",
	}, {
		input: "select /* >> */ 1 from t where a = b >> c",
	}, {
		input:  "select /* % no space */ 1 from t where a = b%c",
		output: "select /* % no space */ 1 from t where a = b % c",
	}, {
		input:  "select /* u+ */ 1 from t where a = +b",
		output: "select /* u+ */ 1 from t where a = b",
	}, {
		input: "select /* u- */ 1 from t where a = -b",
	}, {
		input: "select /* u~ */ 1 from t where a = ~b",
	}, {
		input: "select /* -> */ a.b -> 'ab' from t",
	}, {
		input: "select /* -> */ a.b ->> 'ab' from t",
	}, {
		input: "select /* empty function */ 1 from t where a = b()",
	}, {
		input: "select /* function with 1 param */ 1 from t where a = b(c)",
	}, {
		input: "select /* function with many params */ 1 from t where a = b(c, d)",
	}, {
		input: "select /* function with distinct */ count(distinct a) from t",
	}, {
		input:  "select count(distinctrow(1)) from (select (1) from dual union all select 1 from dual) a",
		output: "select count(distinct 1) from (select 1 from dual union all select 1 from dual) as a",
	}, {
		input: "select /* if as func */ 1 from t where a = if(b)",
	}, {
		input: "select /* current_timestamp */ current_timestamp() from t",
	}, {
		input: "select /* current_timestamp as func */ current_timestamp() from t",
	}, {
		input: "select /* current_timestamp with fsp */ current_timestamp(3) from t",
	}, {
		input: "select /* current_date */ current_date() from t",
	}, {
		input: "select /* current_date as func */ current_date() from t",
	}, {
		input: "select /* current_time */ current_time() from t",
	}, {
		input: "select /* current_time as func */ current_time() from t",
	}, {
		input: "select /* current_time with fsp */ current_time(1) from t",
	}, {
		input: "select /* utc_timestamp */ utc_timestamp() from t",
	}, {
		input: "select /* utc_timestamp as func */ utc_timestamp() from t",
	}, {
		input: "select /* utc_timestamp with fsp */ utc_timestamp(1) from t",
	}, {
		input: "select /* utc_time */ utc_time() from t",
	}, {
		input: "select /* utc_time as func */ utc_time() from t",
	}, {
		input: "select /* utc_time with fsp */ utc_time(4) from t",
	}, {
		input: "select /* utc_date */ utc_date() from t",
	}, {
		input: "select /* utc_date as func */ utc_date() from t",
	}, {
		input: "select /* localtime */ localtime() from t",
	}, {
		input: "select /* localtime as func */ localtime() from t",
	}, {
		input: "select /* localtime with fsp */ localtime(5) from t",
	}, {
		input: "select /* localtimestamp */ localtimestamp() from t",
	}, {
		input: "select /* localtimestamp as func */ localtimestamp() from t",
	}, {
		input: "select /* localtimestamp with fsp */ localtimestamp(7) from t",
	}, {
		input: "select /* mod as func */ a from tab where mod(b, 2) = 0",
	}, {
		input: "select /* database as func no param */ database() from t",
	}, {
		input: "select /* database as func 1 param */ database(1) from t",
	}, {
		input: "select /* a */ a from t",
	}, {
		input: "select /* a.b */ a.b from t",
	}, {
		input: "select /* a.b.c */ a.b.c from t",
	}, {
		input: "select /* keyword a.b */ `By`.`bY` from t",
	}, {
		input: "select /* string */ 'a' from t",
	}, {
		input:  "select /* double quoted string */ \"a\" from t",
		output: "select /* double quoted string */ 'a' from t",
	}, {
		input:  "select /* quote quote in string */ 'a''a' from t",
		output: "select /* quote quote in string */ 'a\\'a' from t",
	}, {
		input:  "select /* double quote quote in string */ \"a\"\"a\" from t",
		output: "select /* double quote quote in string */ 'a\\\"a' from t",
	}, {
		input:  "select /* quote in double quoted string */ \"a'a\" from t",
		output: "select /* quote in double quoted string */ 'a\\'a' from t",
	}, {
		input: "select /* backslash quote in string */ 'a\\'a' from t",
	}, {
		input: "select /* literal backslash in string */ 'a\\\\na' from t",
	}, {
		input: "select /* all escapes */ '\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\' from t",
	}, {
		input:  "select /* non-escape */ '\\x' from t",
		output: "select /* non-escape */ 'x' from t",
	}, {
		input: "select /* unescaped backslash */ '\\n' from t",
	}, {
		input: "select /* value argument */ :a from t",
	}, {
		input: "select /* value argument with digit */ :a1 from t",
	}, {
		input: "select /* value argument with dot */ :a.b from t",
	}, {
		input:  "select /* positional argument */ ? from t",
		output: "select /* positional argument */ :v1 from t",
	}, {
		input:  "select /* multiple positional arguments */ ?, ? from t",
		output: "select /* multiple positional arguments */ :v1, :v2 from t",
	}, {
		input: "select /* list arg */ * from t where a in ::list",
	}, {
		input: "select /* list arg not in */ * from t where a not in ::list",
	}, {
		input: "select /* null */ null from t",
	}, {
		input: "select /* octal */ 010 from t",
	}, {
		input:  "select /* hex */ x'f0A1' from t",
		output: "select /* hex */ X'f0A1' from t",
	}, {
		input: "select /* hex caps */ X'F0a1' from t",
	}, {
		input:  "select /* bit literal */ b'0101' from t",
		output: "select /* bit literal */ B'0101' from t",
	}, {
		input: "select /* bit literal caps */ B'010011011010' from t",
	}, {
		input: "select /* 0x */ 0xf0 from t",
	}, {
		input: "select /* float */ 0.1 from t",
	}, {
		input: "select /* group by */ 1 from t group by a",
	}, {
		input: "select /* having */ 1 from t having a = b",
	}, {
		input:  "select /* simple order by */ 1 from t order by a",
		output: "select /* simple order by */ 1 from t order by a asc",
	}, {
		input:  "select * from t where id = ((select a from t1 union select b from t2) order by a limit 1)",
		output: "select * from t where id = (select a from t1 union select b from t2 order by a asc limit 1)",
	}, {
		input: "select /* order by asc */ 1 from t order by a asc",
	}, {
		input: "select /* order by desc */ 1 from t order by a desc",
	}, {
		input: "select /* order by null */ 1 from t order by null",
	}, {
		input: "select /* limit a */ 1 from t limit a",
	}, {
		input: "select /* limit a,b */ 1 from t limit a, b",
	}, {
		input:  "select /* binary unary */ a- -b from t",
		output: "select /* binary unary */ a - -b from t",
	}, {
		input:  "select /* - - */ - -b from t",
		output: "select /* - - */ - -b from t",
	}, {
		input:  "select /* binary binary */ binary  binary b from t",
		output: "select /* binary binary */ convert(convert(b, binary), binary) from t",
	}, {
		input:  "select /* binary ~ */ binary  ~b from t",
		output: "select /* binary ~ */ convert(~b, binary) from t",
	}, {
		input:  "select /* ~ binary */ ~ binary b from t",
		output: "select /* ~ binary */ ~convert(b, binary) from t",
	}, {
		input: "select /* interval */ adddate('2008-01-02', interval 31 day) from t",
	}, {
		input: "select /* interval keyword */ adddate('2008-01-02', interval 1 year) from t",
	}, {
		input:  "select /* TIMESTAMPADD */ TIMESTAMPADD(MINUTE, 1, '2008-01-04') from t",
		output: "select /* TIMESTAMPADD */ timestampadd(MINUTE, 1, '2008-01-04') from t",
	}, {
		input:  "select /* TIMESTAMPDIFF */ TIMESTAMPDIFF(MINUTE, '2008-01-02', '2008-01-04') from t",
		output: "select /* TIMESTAMPDIFF */ timestampdiff(MINUTE, '2008-01-02', '2008-01-04') from t",
	}, {
		input:  "select DATE_ADD(MIN(FROM_UNIXTIME(1673444922)),interval -DAYOFWEEK(MIN(FROM_UNIXTIME(1673444922)))+1 DAY)",
		output: "select date_add(min(FROM_UNIXTIME(1673444922)), interval -DAYOFWEEK(min(FROM_UNIXTIME(1673444922))) + 1 day) from dual",
	}, {
		input:  "select '2020-01-01' + interval month(DATE_SUB(FROM_UNIXTIME(1234), interval 1 month))-1 month",
		output: "select '2020-01-01' + interval month(date_sub(FROM_UNIXTIME(1234), interval 1 month)) - 1 month from dual",
	}, {
		input: "select /* dual */ 1 from dual",
	}, {
		input:  "select /* Dual */ 1 from Dual",
		output: "select /* Dual */ 1 from dual",
	}, {
		input:  "select /* DUAL */ 1 from Dual",
		output: "select /* DUAL */ 1 from dual",
	}, {
		input: "select /* column as bool in where */ a from t where b",
	}, {
		input: "select /* OR of columns in where */ * from t where a or b",
	}, {
		input: "select /* OR of mixed columns in where */ * from t where a = 5 or b and c is not null",
	}, {
		input:  "select /* OR in select columns */ (a or b) from t where c = 5",
		output: "select /* OR in select columns */ a or b from t where c = 5",
	}, {
		input: "select /* XOR of columns in where */ * from t where a xor b",
	}, {
		input: "select /* XOR of mixed columns in where */ * from t where a = 5 xor b and c is not null",
	}, {
		input:  "select /* XOR in select columns */ (a xor b) from t where c = 5",
		output: "select /* XOR in select columns */ a xor b from t where c = 5",
	}, {
		input:  "select /* XOR in select columns */ * from t where (1 xor c1 > 0)",
		output: "select /* XOR in select columns */ * from t where 1 xor c1 > 0",
	}, {
		input: "select /* bool as select value */ a, true from t",
	}, {
		input: "select /* bool column in ON clause */ * from t join s on t.id = s.id and s.foo where t.bar",
	}, {
		input: "select /* bool in order by */ * from t order by a is null or b asc",
	}, {
		input: "select /* string in case statement */ if(max(case a when 'foo' then 1 else 0 end) = 1, 'foo', 'bar') as foobar from t",
	}, {
		input:  "/*!show databases*/",
		output: "show databases",
	}, {
		input:  "select /*!40101 * from*/ t",
		output: "select * from t",
	}, {
		input:  "select /*! * from*/ t",
		output: "select * from t",
	}, {
		input:  "select /*!* from*/ t",
		output: "select * from t",
	}, {
		input:  "select /*!401011 from*/ t",
		output: "select 1 from t",
	}, {
		input: "select /* dual */ 1 from dual",
	}, {
		input:  "select * from (select 'tables') tables",
		output: "select * from (select 'tables' from dual) as `tables`",
	}, {
		input: "insert /* simple */ into a values (1)",
	}, {
		input: "insert /* a.b */ into a.b values (1)",
	}, {
		input: "insert /* multi-value */ into a values (1, 2)",
	}, {
		input: "insert /* multi-value list */ into a values (1, 2), (3, 4)",
	}, {
		input: "insert /* no values */ into a values ()",
	}, {
		input:  "insert /* set */ into a set a = 1, b = 2",
		output: "insert /* set */ into a(a, b) values (1, 2)",
	}, {
		input:  "insert /* set default */ into a set a = default, b = 2",
		output: "insert /* set default */ into a(a, b) values (default, 2)",
	}, {
		input: "insert /* value expression list */ into a values (a + 1, 2 * 3)",
	}, {
		input: "insert /* default */ into a values (default, 2 * 3)",
	}, {
		input: "insert /* column list */ into a(a, b) values (1, 2)",
	}, {
		input: "insert into a(a, b) values (1, ifnull(null, default(b)))",
	}, {
		input: "insert /* qualified column list */ into a(a, b) values (1, 2)",
	}, {
		input:  "insert /* qualified columns */ into t (t.a, t.b) values (1, 2)",
		output: "insert /* qualified columns */ into t(a, b) values (1, 2)",
	}, {
		input: "insert /* select */ into a select b, c from d",
	}, {
		input:  "insert /* it accepts columns with keyword action */ into a(action, b) values (1, 2)",
		output: "insert /* it accepts columns with keyword action */ into a(`action`, b) values (1, 2)",
	}, {
		input:  "insert /* no cols & paren select */ into a (select * from t)",
		output: "insert /* no cols & paren select */ into a select * from t",
	}, {
		input:  "insert /* cols & paren select */ into a(a, b, c) (select * from t)",
		output: "insert /* cols & paren select */ into a(a, b, c) select * from t",
	}, {
		input:  "insert /* cols & union with paren select */ into a(b, c) (select d, e from f) union (select g from h)",
		output: "insert /* cols & union with paren select */ into a(b, c) select d, e from f union select g from h",
	}, {
		input: "insert /* on duplicate */ into a values (1, 2) on duplicate key update b = func(a), c = d",
	}, {
		input: "insert /* bool in insert value */ into a values (1, true, false)",
	}, {
		input: "insert /* bool in on duplicate */ into a values (1, 2) on duplicate key update b = false, c = d",
	}, {
		input: "insert /* bool in on duplicate */ into a values (1, 2, 3) on duplicate key update b = values(b), c = d",
	}, {
		input: "insert /* bool in on duplicate */ into a values (1, 2, 3) on duplicate key update b = values(a.b), c = d",
	}, {
		input: "insert /* bool expression on duplicate */ into a values (1, 2) on duplicate key update b = func(a), c = a > d",
	}, {
		input: "insert into `user`(username, `status`) values ('Chuck', default(`status`))",
	}, {
		input:  "insert into user(format, tree, vitess) values ('Chuck', 42, 'Barry')",
		output: "insert into `user`(`format`, `tree`, `vitess`) values ('Chuck', 42, 'Barry')",
	}, {
		input: "insert into customer() values ()",
	}, {
		input: "update /* simple */ a set b = 3",
	}, {
		input: "update /* a.b */ a.b set b = 3",
	}, {
		input: "update a.b set d = @v := d + 7 where u = 42",
	}, {
		input:  "select @topic3_id:= 10103;",
		output: "select @topic3_id := 10103 from dual",
	}, {
		input: "update /* list */ a set b = 3, c = 4",
	}, {
		input: "update /* expression */ a set b = 3 + 4",
	}, {
		input: "update /* where */ a set b = 3 where a = b",
	}, {
		input: "update /* order */ a set b = 3 order by c desc",
	}, {
		input: "update /* limit */ a set b = 3 limit c",
	}, {
		input: "update /* bool in update */ a set b = true",
	}, {
		input: "update /* bool expr in update */ a set b = 5 > 2",
	}, {
		input: "update /* bool in update where */ a set b = 5 where c",
	}, {
		input: "update /* table qualifier */ a set a.b = 3",
	}, {
		input: "update /* table qualifier */ a set t.a.b = 3",
	}, {
		input:  "update /* table alias */ tt aa set aa.cc = 3",
		output: "update /* table alias */ tt as aa set aa.cc = 3",
	}, {
		input:  "update (select id from foo) subqalias set id = 4",
		output: "update (select id from foo) as subqalias set id = 4",
	}, {
		input:  "update foo f, bar b set f.id = b.id where b.name = 'test'",
		output: "update foo as f, bar as b set f.id = b.id where b.`name` = 'test'",
	}, {
		input:  "update foo f join bar b on f.name = b.name set f.id = b.id where b.name = 'test'",
		output: "update foo as f join bar as b on f.`name` = b.`name` set f.id = b.id where b.`name` = 'test'",
	}, {
		input: "update /* ignore */ ignore a set b = 3",
	}, {
		input: "delete /* simple */ from a",
	}, {
		input: "delete /* a.b */ from a.b",
	}, {
		input: "delete /* where */ from a where a = b",
	}, {
		input: "delete /* order */ from a order by b desc",
	}, {
		input: "delete /* limit */ from a limit b",
	}, {
		input:  "delete /* alias where */ t.* from a as t where t.id = 2",
		output: "delete /* alias where */ t from a as t where t.id = 2",
	}, {
		input:  "delete t.* from t, t1",
		output: "delete t from t, t1",
	}, {
		input:  "delete a from a join b on a.id = b.id where b.name = 'test'",
		output: "delete a from a join b on a.id = b.id where b.`name` = 'test'",
	}, {
		input:  "delete a, b from a, b where a.id = b.id and b.name = 'test'",
		output: "delete a, b from a, b where a.id = b.id and b.`name` = 'test'",
	}, {
		input: "delete /* simple */ ignore from a",
	}, {
		input: "delete ignore from a",
	}, {
		input: "delete /* limit */ ignore from a",
	}, {
		input:  "delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id=a2.id",
		output: "delete a1, a2 from t1 as a1 join t2 as a2 where a1.id = a2.id",
	}, {
		input:  "set /* simple */ a = 3",
		output: "set /* simple */ @@a = 3",
	}, {
		input:  "set #simple\n b = 4",
		output: "set #simple\n @@b = 4",
	}, {
		input:  "set character_set_results = utf8",
		output: "set @@character_set_results = utf8",
	}, {
		input:  "set @@session.autocommit = true",
		output: "set @@autocommit = true",
	}, {
		input:  "set @@session.`autocommit` = true",
		output: "set @@autocommit = true",
	}, {
		input:  "set @@session.autocommit = ON",
		output: "set @@autocommit = 'on'",
	}, {
		input:  "set @@session.autocommit= OFF",
		output: "set @@autocommit = 'off'",
	}, {
		input:  "set autocommit = on",
		output: "set @@autocommit = 'on'",
	}, {
		input:  "set autocommit = off",
		output: "set @@autocommit = 'off'",
	}, {
		input:  "set names utf8 collate foo",
		output: "set names 'utf8'",
	}, {
		input:  "set names utf8 collate 'foo'",
		output: "set names 'utf8'",
	}, {
		input:  "set character set utf8",
		output: "set charset 'utf8'",
	}, {
		input:  "set character set 'utf8'",
		output: "set charset 'utf8'",
	}, {
		input:  "set s = 1--4",
		output: "set @@s = 1 - -4",
	}, {
		input:  "set character set \"utf8\"",
		output: "set charset 'utf8'",
	}, {
		input:  "set charset default",
		output: "set charset default",
	}, {
		input:  "set session wait_timeout = 3600",
		output: "set @@wait_timeout = 3600",
	}, {
		input:  "set session wait_timeout = 3600, session autocommit = off",
		output: "set @@wait_timeout = 3600, @@autocommit = 'off'",
	}, {
		input:  "set session wait_timeout = 3600, @@global.autocommit = off",
		output: "set @@wait_timeout = 3600, @@global.autocommit = 'off'",
	}, {
		input:  "set local wait_timeout = 3600",
		output: "set @@wait_timeout = 3600",
	}, {
		input:  "set @@local.wait_timeout = 3600",
		output: "set @@wait_timeout = 3600",
	}, {
		input:  "set /* list */ a = 3, b = 4",
		output: "set /* list */ @@a = 3, @@b = 4",
	}, {
		input:  "set /* mixed list */ a = 3, names 'utf8', charset 'ascii', b = 4",
		output: "set /* mixed list */ @@a = 3, names 'utf8', charset 'ascii', @@b = 4",
	}, {
		input:  "set session transaction isolation level repeatable read",
		output: "set @@session.transaction_isolation = 'repeatable-read'",
	}, {
		input:  "set transaction isolation level repeatable read",
		output: "set @@transaction_isolation = 'repeatable-read'",
	}, {
		input:  "set global transaction isolation level repeatable read",
		output: "set @@global.transaction_isolation = 'repeatable-read'",
	}, {
		input:  "set transaction isolation level repeatable read",
		output: "set @@transaction_isolation = 'repeatable-read'",
	}, {
		input:  "set transaction isolation level read committed",
		output: "set @@transaction_isolation = 'read-committed'",
	}, {
		input:  "set transaction isolation level read uncommitted",
		output: "set @@transaction_isolation = 'read-uncommitted'",
	}, {
		input:  "set transaction isolation level serializable",
		output: "set @@transaction_isolation = 'serializable'",
	}, {
		input:  "set transaction read write",
		output: "set @@transaction_read_only = 'off'",
	}, {
		input:  "set transaction read only",
		output: "set @@transaction_read_only = 'on'",
	}, {
		input:  "set session transaction read only, isolation level serializable",
		output: "set @@session.transaction_read_only = 'on', @@session.transaction_isolation = 'serializable'",
	}, {
		input:  "set tx_read_only = 1",
		output: "set @@tx_read_only = 1",
	}, {
		input:  "set tx_read_only = 0",
		output: "set @@tx_read_only = 0",
	}, {
		input:  "set transaction_read_only = 1",
		output: "set @@session.transaction_read_only = 1",
	}, {
		input:  "set transaction_read_only = 0",
		output: "set @@session.transaction_read_only = 0",
	}, {
		input: "set @@transaction_read_only = 1",
	}, {
		input: "set @@transaction_isolation = 'read-committed'",
	}, {
		input:  "set tx_isolation = 'repeatable read'",
		output: "set @@tx_isolation = 'repeatable read'",
	}, {
		input:  "set tx_isolation = 'read committed'",
		output: "set @@tx_isolation = 'read committed'",
	}, {
		input:  "set tx_isolation = 'read uncommitted'",
		output: "set @@tx_isolation = 'read uncommitted'",
	}, {
		input:  "set tx_isolation = 'serializable'",
		output: "set @@tx_isolation = 'serializable'",
	}, {
		input:  "set sql_safe_updates = 0",
		output: "set @@sql_safe_updates = 0",
	}, {
		input:  "set sql_safe_updates = 1",
		output: "set @@sql_safe_updates = 1",
	}, {
		input: "set @variable = 42",
	}, {
		input:  "set @period.variable = 42",
		output: "set @`period.variable` = 42",
	}, {
		input:  "set S= +++-++-+(4+1)",
		output: "set @@S = - -(4 + 1)",
	}, {
		input:  "set S= +- - - - -(4+1)",
		output: "set @@S = - - - - -(4 + 1)",
	}, {
		input:  "alter table a add foo int references b (a) on delete restrict first",
		output: "alter table a add column foo int references b (a) on delete restrict first",
	}, {
		input:  "alter table a lock default, lock = none, lock shared, lock exclusive",
		output: "alter table a lock default, lock none, lock shared, lock exclusive",
	}, {
		input:  "alter table a alter x set default NULL, alter column x2 set default 's', alter x3 drop default",
		output: "alter table a alter column x set default null, alter column x2 set default 's', alter column x3 drop default",
	}, {
		input: "alter table a alter column x set visible, alter column x2 set invisible",
	}, {
		input: "alter table a alter index x visible, alter index x2 invisible",
	}, {
		input: "alter table a add spatial key foo (column1)",
	}, {
		input: "alter table a add fulltext key foo (column1), order by a, b, c",
	}, {
		input: "alter table a add unique key foo (column1)",
	}, {
		input: "alter /*vt+ strategy=online */ table a add unique key foo (column1)",
	}, {
		input: "alter table a change column s foo int default 1 after x",
	}, {
		input: "alter table a modify column foo int default 1 first",
	}, {
		input: "alter table a rename column foo to bar",
	}, {
		input:  "alter table a add foo varchar(255) generated always as (concat(bar, ' ', baz)) stored",
		output: "alter table a add column foo varchar(255) as (concat(bar, ' ', baz)) stored",
	}, {
		input:  "alter table a add foo varchar(255) generated always as (concat(bar, ' ', baz))",
		output: "alter table a add column foo varchar(255) as (concat(bar, ' ', baz)) virtual",
	}, {
		input:  "alter table a add foo varchar(255) generated always as (concat(bar, ' ', baz)) null",
		output: "alter table a add column foo varchar(255) as (concat(bar, ' ', baz)) virtual null",
	}, {
		input:  "alter table a add foo varchar(255) generated always as (concat(bar, ' ', baz)) not null",
		output: "alter table a add column foo varchar(255) as (concat(bar, ' ', baz)) virtual not null",
	}, {
		input:  "alter table a change column s foo varchar(255) generated always as (concat(bar, ' ', baz)) stored",
		output: "alter table a change column s foo varchar(255) as (concat(bar, ' ', baz)) stored",
	}, {
		input:  "alter table a modify column foo varchar(255) generated always as (concat(bar, ' ', baz))",
		output: "alter table a modify column foo varchar(255) as (concat(bar, ' ', baz)) virtual",
	}, {
		input:  "alter table a character set utf32 collate = 'utf'",
		output: "alter table a charset utf32 collate utf",
	}, {
		input: "alter table a convert to character set utf32",
	}, {
		input: "alter table `By` add column foo int, algorithm = default",
	}, {
		input: "alter table `By` add column foo int, algorithm = copy",
	}, {
		input: "alter table `By` add column foo int, algorithm = inplace",
	}, {
		input: "alter table `By` add column foo int, algorithm = INPLACE",
	}, {
		input: "alter table `By` add column foo int, algorithm = instant",
	}, {
		input: "alter table a rename b",
	}, {
		input: "alter table `By` rename `bY`",
	}, {
		input:  "alter table a rename to b",
		output: "alter table a rename b",
	}, {
		input:  "alter table a rename as b",
		output: "alter table a rename b",
	}, {
		input: "alter table a rename index foo to bar, with validation",
	}, {
		input:  "alter table a rename key foo to bar",
		output: "alter table a rename index foo to bar",
	}, {
		input: "alter table e auto_increment 20",
	}, {
		input:  "alter table e character set = 'ascii'",
		output: "alter table e charset ascii",
	}, {
		input: "alter table e enable keys, discard tablespace, force",
	}, {
		input:  "alter table e default character set = 'ascii'",
		output: "alter table e charset ascii",
	}, {
		input: "alter table e comment 'hello' remove partitioning",
	}, {
		input:  "alter table a reorganize partition b into (partition c values less than (?), partition d values less than (maxvalue))",
		output: "alter table a reorganize partition b into (partition c values less than (:v1), partition d values less than maxvalue)",
	}, {
		input: "alter table a algorithm = default, lock none, add partition (partition d values less than maxvalue)",
	}, {
		input: "alter table a discard partition all tablespace",
	}, {
		input: "alter table a import partition a, b, v tablespace",
	}, {
		input: "alter table a truncate partition a, b, v",
	}, {
		input: "alter table a coalesce partition 7",
	}, {
		input: "alter table a exchange partition a with table t without validation",
	}, {
		input: "alter table a analyze partition all",
	}, {
		input: "alter table a analyze partition a, v",
	}, {
		input: "alter table a check partition all",
	}, {
		input: "alter table a check partition a, v",
	}, {
		input: "alter table a optimize partition all",
	}, {
		input: "alter table a optimize partition a, v",
	}, {
		input: "alter table a rebuild partition all",
	}, {
		input: "alter table a rebuild partition a, v",
	}, {
		input: "alter table a repair partition all",
	}, {
		input: "alter table a repair partition a, v",
	}, {
		input: "alter table a remove partitioning",
	}, {
		input: "alter table a upgrade partitioning",
	}, {
		input:  "alter table t2 add primary key `zzz` (id)",
		output: "alter table t2 add primary key (id)",
	}, {
		input: "alter table a \npartition by hash (id) partitions 4",
	}, {
		input: "alter table a \npartition by range (id)\n(partition p0 values less than (10),\n partition p1 values less than maxvalue)",
	}, {
		input:      "create database a garbage values",
		output:     "create database a",
		partialDDL: true,
	}, {
		input: "alter table `Post With Space` drop foreign key `Post With Space_ibfk_1`",
	}, {
		input: "alter table a add column (id int, id2 char(23))",
	}, {
		input: "alter table a add index idx (id)",
	}, {
		input: "alter table a add fulltext index idx (id)",
	}, {
		input: "alter table a add spatial index idx (id)",
	}, {
		input: "alter table a add fulltext index idx (id)",
	}, {
		input: "alter table a add foreign key (id) references f (id)",
	}, {
		input: "alter table a add foreign key the_idx(id) references f (id)",
	}, {
		input: "alter table a add primary key (id)",
	}, {
		input: "alter table a add constraint b primary key (id)",
	}, {
		input: "alter table a add constraint b primary key (id)",
	}, {
		input: "alter table a add constraint b unique key (id)",
	}, {
		input:  "alter table t add column iii int signed not null",
		output: "alter table t add column iii int not null",
	}, {
		input: "alter table t add column iii int unsigned not null",
	}, {
		input:  "alter table a add constraint b unique c (id)",
		output: "alter table a add constraint b unique key c (id)",
	}, {
		input:  "alter table a add constraint check (id)",
		output: "alter table a add check (id)",
	}, {
		input: "alter table a add constraint c check (id)",
	}, {
		input:  "alter table a add id int",
		output: "alter table a add column id int",
	}, {
		input: "alter table a add column id int first",
	}, {
		input: "alter table a add column id int after id2",
	}, {
		input: "alter table a drop column id",
	}, {
		input: "alter table a drop partition p2712, p123",
	}, {
		input:  "alter table a drop index idx",
		output: "alter table a drop key idx",
	}, {
		input: "alter table a add check (ch_1) not enforced",
	}, {
		input: "alter table a alter check ch_1 enforced",
	}, {
		input: "alter table a alter check ch_1 not enforced",
	}, {
		input: "alter table a drop check ch_1",
	}, {
		input:  "alter table a drop constraint ch_1",
		output: "alter table a drop check ch_1",
	}, {
		input: "alter table a drop foreign key kx",
	}, {
		input: "alter table a drop primary key",
	}, {
		input:  "alter table a drop id",
		output: "alter table a drop column id",
	}, {
		input:  "ALTER TABLE `product115s` CHANGE `part_number` `part_number` varchar(255) DEFAULT '0' NOT NULL",
		output: "alter table product115s change column part_number part_number varchar(255) not null default '0'",
	}, {
		input:  "ALTER TABLE distributors ADD CONSTRAINT zipchk CHECK (char_length(zipcode) = 5)",
		output: "alter table distributors add constraint zipchk check (char_length(zipcode) = 5)",
	}, {
		input: "alter database character set geostd8",
	}, {
		input: "alter database d character set geostd8",
	}, {
		input: "alter database d default collate 'utf8_bin'",
	}, {
		input: "alter database default collate 'utf8_bin'",
	}, {
		input: "alter database d upgrade data directory name",
	}, {
		input:  "alter database d collate = 'utf8_bin'",
		output: "alter database d collate 'utf8_bin'",
	}, {
		input:  "alter schema d default character set = geostd8",
		output: "alter database d default character set geostd8",
	}, {
		input:  "alter schema d character set = geostd8",
		output: "alter database d character set geostd8",
	}, {
		input:  "alter schema d default collate = 'utf8_bin'",
		output: "alter database d default collate 'utf8_bin'",
	}, {
		input:  "alter schema d collate = 'utf8_bin' character set = geostd8 character set = geostd8",
		output: "alter database d collate 'utf8_bin' character set geostd8 character set geostd8",
	}, {
		input:      "create table a",
		partialDDL: true,
	}, {
		input:      "CREATE TABLE a",
		output:     "create table a",
		partialDDL: true,
	}, {
		input:      "create table `a`",
		output:     "create table a",
		partialDDL: true,
	}, {
		input:  "create table function_default (x varchar(25) default (trim(' check ')))",
		output: "create table function_default (\n\tx varchar(25) default (trim(' check '))\n)",
	}, {
		input:  "create table function_default (x varchar(25) default (((trim(' check ')))))",
		output: "create table function_default (\n\tx varchar(25) default (trim(' check '))\n)",
	}, {
		input:  "create table function_default3 (x bool DEFAULT (true AND false));",
		output: "create table function_default3 (\n\tx bool default (true and false)\n)",
	}, {
		input:  "create table function_default (x bool DEFAULT true);",
		output: "create table function_default (\n\tx bool default true\n)",
	}, {
		input:  "create table a (\n\t`a` int\n)",
		output: "create table a (\n\ta int\n)",
	}, {
		input: "create table `by` (\n\t`by` char\n)",
	}, {
		input: "create table test (\n\t__year year(4)\n)",
	}, {
		input: "create table a (\n\ta int not null\n)",
	}, {
		input: "create /*vt+ strategy=online */ table a (\n\ta int not null\n)",
	}, {
		input: "create table a (\n\ta int not null default 0\n)",
	}, {
		input:  "create table a (\n\ta float not null default -1\n)",
		output: "create table a (\n\ta float not null default -1\n)",
	}, {
		input:  "create table a (\n\ta float not null default -2.1\n)",
		output: "create table a (\n\ta float not null default -2.1\n)",
	}, {
		input:  "create table a (\n\ta float(24) not null default -1\n)",
		output: "create table a (\n\ta float(24) not null default -1\n)",
	}, {
		input:  "create table a (\n\ta float(24,10) not null default -1\n)",
		output: "create table a (\n\ta float(24,10) not null default -1\n)",
	}, {
		input:  "create table a (\n\ta float4 not null default -1\n)",
		output: "create table a (\n\ta float4 not null default -1\n)",
	}, {
		input:  "create table a (\n\ta float8 not null default -1\n)",
		output: "create table a (\n\ta float8 not null default -1\n)",
	}, {
		input:  "create table a (a int not null default 0, primary key(a))",
		output: "create table a (\n\ta int not null default 0,\n\tprimary key (a)\n)",
	}, {
		input:  "create table a (`a column` int)",
		output: "create table a (\n\t`a column` int\n)",
	}, {
		input: "create table a (\n\ta varchar(32) not null default ''\n)",
	}, {
		input:  "create table if not exists a (\n\t`a` int\n)",
		output: "create table if not exists a (\n\ta int\n)",
	}, {
		input:      "create table a ignore me this is garbage",
		output:     "create table a",
		partialDDL: true,
	}, {
		input:      "create table a (a int, b char, c garbage)",
		output:     "create table a",
		partialDDL: true,
	}, {
		input:  "create table a (b1 bool not null primary key, b2 boolean not null)",
		output: "create table a (\n\tb1 bool not null primary key,\n\tb2 boolean not null\n)",
	}, {
		input:  "create table a (b1 bool NOT NULL PRIMARY KEY, b2 boolean not null references b (a) on delete restrict, KEY b2_idx(b))",
		output: "create table a (\n\tb1 bool not null primary key,\n\tb2 boolean not null references b (a) on delete restrict,\n\tKEY b2_idx (b)\n)",
	}, {
		input: "create temporary table a (\n\tid bigint\n)",
	}, {
		input:  "CREATE TABLE pkai (id INT PRIMARY KEY AUTO_INCREMENT);",
		output: "create table pkai (\n\tid INT auto_increment primary key\n)",
	}, {
		input:  "CREATE TABLE aipk (id INT AUTO_INCREMENT PRIMARY KEY)",
		output: "create table aipk (\n\tid INT auto_increment primary key\n)",
	}, {
		// This test case is added because MySQL supports this behaviour.
		// It allows the user to specify null and not null multiple times.
		// The last value specified is used.
		input:  "create table foo (f timestamp null not null , g timestamp not null null)",
		output: "create table foo (\n\tf timestamp not null,\n\tg timestamp null\n)",
	}, {
		// Tests unicode character §
		input: "create table invalid_enum_value_name (\n\there_be_enum enum('$§!') default null\n)",
	}, {
		input:  "create table t (id int) partition by hash (id) partitions 3",
		output: "create table t (\n\tid int\n)\npartition by hash (id) partitions 3",
	}, {
		input:  "create table t (hired date) partition by linear hash (year(hired)) partitions 4",
		output: "create table t (\n\thired date\n)\npartition by linear hash (year(hired)) partitions 4",
	}, {
		input:  "create table t (id int) partition by key (id) partitions 2",
		output: "create table t (\n\tid int\n)\npartition by key (id) partitions 2",
	}, {
		input:  "create table t (id int, primary key(id)) partition by key () partitions 2",
		output: "create table t (\n\tid int,\n\tprimary key (id)\n)\npartition by key () partitions 2",
	}, {
		input:  "create table t (id int) partition by key algorithm = 1 (id)",
		output: "create table t (\n\tid int\n)\npartition by key algorithm = 1 (id)",
	}, {
		input:  "create table t (id int not null) partition by linear key (id) partitions 5",
		output: "create table t (\n\tid int not null\n)\npartition by linear key (id) partitions 5",
	}, {
		input:  "create table t (id int) partition by list (id) (partition p0 values in (1, 4, 7))",
		output: "create table t (\n\tid int\n)\npartition by list (id)\n(partition p0 values in (1, 4, 7))",
	}, {
		input:  "create table t (renewal date) partition by range columns (renewal) (partition p0 values less than ('2021-08-27'))",
		output: "create table t (\n\trenewal date\n)\npartition by range columns (renewal)\n(partition p0 values less than ('2021-08-27'))",
	}, {
		input:  "create table t (pur date) partition by range (year(pur)) subpartition by hash (to_days(pur)) subpartitions 2 (partition p0 values less than (2015), partition p2 values less than (2018))",
		output: "create table t (\n\tpur date\n)\npartition by range (year(pur)) subpartition by hash (to_days(pur)) subpartitions 2\n(partition p0 values less than (2015),\n partition p2 values less than (2018))",
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema create vindex hash_vdx using `hash`",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema create vindex keyspace.hash_vdx using `hash`",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema create vindex lookup_vdx using lookup with owner=user, table=name_user_idx, from=name, to=user_id",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema create vindex xyz_vdx using xyz with param1=hello, param2='world', param3=123",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema drop vindex hash_vdx",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema drop vindex ks.hash_vdx",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema add table a",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema add table ks.a",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema add sequence a_seq",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema add sequence ks.a_seq",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a add auto_increment id using a_seq",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on ks.a add auto_increment id using a_seq",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema drop table a",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema drop table ks.a",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a add vindex `hash` (id)",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on ks.a add vindex `hash` (id)",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a add vindex `hash` (`id`)",
		output:               "alter vschema on a add vindex `hash` (id)",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on `ks`.a add vindex `hash` (`id`)",
		output:               "alter vschema on ks.a add vindex `hash` (id)",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a add vindex hash (id) using `hash`",
		output:               "alter vschema on a add vindex `hash` (id) using `hash`",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a add vindex `add` (`add`)",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a add vindex `hash` (id) using `hash`",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a add vindex hash (id) using `hash`",
		output:               "alter vschema on a add vindex `hash` (id) using `hash`",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on user add vindex name_lookup_vdx (name) using lookup_hash with owner=user, table=name_user_idx, from=name, to=user_id",
		output:               "alter vschema on `user` add vindex name_lookup_vdx (`name`) using lookup_hash with owner=user, table=name_user_idx, from=name, to=user_id",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on user2 add vindex name_lastname_lookup_vdx (name,lastname) using lookup with owner=`user`, table=`name_lastname_keyspace_id_map`, from=`name,lastname`, to=`keyspace_id`",
		output:               "alter vschema on user2 add vindex name_lastname_lookup_vdx (`name`, lastname) using lookup with owner=user, table=name_lastname_keyspace_id_map, from=name,lastname, to=keyspace_id",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a drop vindex `hash`",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on ks.a drop vindex `hash`",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a drop vindex `hash`",
		output:               "alter vschema on a drop vindex `hash`",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a drop vindex hash",
		output:               "alter vschema on a drop vindex `hash`",
		ignoreNormalizerTest: true,
	}, {
		// Alter Vschema does not reach the vttablets, so we don't need to run the normalizer test
		input:                "alter vschema on a drop vindex `add`",
		output:               "alter vschema on a drop vindex `add`",
		ignoreNormalizerTest: true,
	}, {
		input:  "create index a on b (col1)",
		output: "alter table b add index a (col1)",
	}, {
		input:  "create unique index a on b (col1)",
		output: "alter table b add unique index a (col1)",
	}, {
		input:  "create unique index a using foo on b (col1 desc)",
		output: "alter table b add unique index a (col1 desc) using foo",
	}, {
		input:  "create fulltext index a on b (col1) with parser a",
		output: "alter table b add fulltext index a (col1) with parser a",
	}, {
		input:  "create spatial index a on b (col1)",
		output: "alter table b add spatial index a (col1)",
	}, {
		input:  "create fulltext index a on b (col1) key_block_size=12 with parser a comment 'string' algorithm inplace lock none",
		output: "alter table b add fulltext index a (col1) key_block_size 12 with parser a comment 'string', algorithm = inplace, lock none",
	}, {
		input:  "create index a on b ((col1 + col2), (col1*col2))",
		output: "alter table b add index a ((col1 + col2), (col1 * col2))",
	}, {
		input:  "create fulltext index b using btree on A (col1 desc, col2) algorithm = inplace lock = none",
		output: "alter table A add fulltext index b (col1 desc, col2) using btree, algorithm = inplace, lock none",
	}, {
		input: "create algorithm = merge sql security definer view a as select * from e",
	}, {
		input: "create view ks.a as select * from e",
	}, {
		input:  "create algorithm = merge sql security definer view a (b,c,d) as select * from e",
		output: "create algorithm = merge sql security definer view a(b, c, d) as select * from e",
	}, {
		input:  "create algorithm = merge sql security definer view a (b,c,d) as select * from e with cascaded check option",
		output: "create algorithm = merge sql security definer view a(b, c, d) as select * from e with cascaded check option",
	}, {
		input:  "create algorithm = temptable definer = a@b.c.d view a(b,c,d) as select * from e with local check option",
		output: "create algorithm = temptable definer = a@`b.c.d` view a(b, c, d) as select * from e with local check option",
	}, {
		input:  "create algorithm = temptable definer = a@b view a(b,c,d) as select * from e with local check option",
		output: "create algorithm = temptable definer = a@b view a(b, c, d) as select * from e with local check option",
	}, {
		input:  "create algorithm = temptable definer = 'create'@b view a(b,c,d) as select * from e with local check option",
		output: "create algorithm = temptable definer = 'create'@b view a(b, c, d) as select * from e with local check option",
	}, {
		input:  "create algorithm = temptable definer = a@'create' view a(b,c,d) as select * from e with local check option",
		output: "create algorithm = temptable definer = a@'create' view a(b, c, d) as select * from e with local check option",
	}, {
		input:  "create algorithm = temptable definer = 'a' view a(b,c,d) as select * from e with local check option",
		output: "create algorithm = temptable definer = 'a' view a(b, c, d) as select * from e with local check option",
	}, {
		input:  "create algorithm = temptable definer = 'select'@'create' view a(b,c,d) as select * from e with local check option",
		output: "create algorithm = temptable definer = 'select'@'create' view a(b, c, d) as select * from e with local check option",
	}, {
		input:  "create algorithm = temptable definer = `create`@b view a(b,c,d) as select * from e with local check option",
		output: "create algorithm = temptable definer = `create`@b view a(b, c, d) as select * from e with local check option",
	}, {
		input:  "create algorithm = temptable definer = a@`create` view a(b,c,d) as select * from e with local check option",
		output: "create algorithm = temptable definer = a@`create` view a(b, c, d) as select * from e with local check option",
	}, {
		input:  "create algorithm = temptable definer = `select`@`create` view a(b,c,d) as select * from e with local check option",
		output: "create algorithm = temptable definer = `select`@`create` view a(b, c, d) as select * from e with local check option",
	}, {
		input:  "create or replace algorithm = temptable definer = a@b.c.d sql security definer view a(b,c,d) as select * from e with local check option",
		output: "create or replace algorithm = temptable definer = a@`b.c.d` sql security definer view a(b, c, d) as select * from e with local check option",
	}, {
		input:  "create algorithm = undefined definer = `msandbox`@`localhost` sql security definer view `v3` as select `t`.`id` as `id` from `t`",
		output: "create algorithm = undefined definer = msandbox@localhost sql security definer view v3 as select t.id as id from t",
	}, {
		input:  "create definer = 'sa'@b.c.d view a(b,c,d) as select * from e",
		output: "create definer = 'sa'@`b.c.d` view a(b, c, d) as select * from e",
	}, {
		input: "create /*vt+ strategy=online */ or replace view v as select a, b, c from t",
	}, {
		input: "alter view a as select * from t",
	}, {
		input: "alter /*vt+ strategy=online */ view a as select * from t",
	}, {
		input: "alter algorithm = merge definer = m@`172.0.1.01` sql security definer view a as select * from t with local check option",
	}, {
		input:  "rename table a to b",
		output: "rename table a to b",
	}, {
		input:  "rename table x.a to b, b to c",
		output: "rename table x.a to b, b to c",
	}, {
		input:  "drop view a,B,c",
		output: "drop view a, B, c",
	}, {
		input: "drop /*vt+ strategy=online */ view if exists v",
	}, {
		input: "drop table a",
	}, {
		input: "drop /*vt+ strategy=online */ table if exists a",
	}, {
		input: "drop /*vt+ strategy=online */ table a",
	}, {
		input:  "drop table a, b",
		output: "drop table a, b",
	}, {
		input:  "drop table if exists a,b restrict",
		output: "drop table if exists a, b",
	}, {
		input: "drop temporary table if exists a, b",
	}, {
		input:  "drop view if exists a cascade",
		output: "drop view if exists a",
	}, {
		input:  "drop index b on a lock = none algorithm default",
		output: "alter table a drop key b, lock none, algorithm = default",
	}, {
		input:  "drop index `PRIMARY` on a lock none",
		output: "alter table a drop primary key, lock none",
	}, {
		input:  "analyze table a",
		output: "otherread",
	}, {
		input: "flush tables",
	}, {
		input: "flush tables with read lock",
	}, {
		input: "flush tables a, c.v, b",
	}, {
		input: "flush local tables a, c.v, b with read lock",
	}, {
		input: "flush tables a, c.v, b for export",
	}, {
		input: "flush local binary logs, engine logs, error logs, general logs, hosts, logs, privileges, optimizer_costs",
	}, {
		input:  "flush no_write_to_binlog slow logs, status, user_resources, relay logs, relay logs for channel s",
		output: "flush local slow logs, status, user_resources, relay logs, relay logs for channel s",
	}, {
		input: "show binary logs",
	}, {
		input:  "show binlog events",
		output: "show binlog",
	}, {
		input: "purge binary logs to 'x'",
	}, {
		input: "purge binary logs before '2020-02-02 20:20:20'",
	}, {
		input:  "show character set",
		output: "show charset",
	}, {
		input:  "show character set like '%foo'",
		output: "show charset like '%foo'",
	}, {
		input:  "show charset",
		output: "show charset",
	}, {
		input:  "show charset like '%foo'",
		output: "show charset like '%foo'",
	}, {
		input:  "show charset where 'charset' = 'utf8'",
		output: "show charset where 'charset' = 'utf8'",
	}, {
		input:  "show charset where 'charset' = '%foo'",
		output: "show charset where 'charset' = '%foo'",
	}, {
		input:  "show collation",
		output: "show collation",
	}, {
		input:  "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'",
		output: "show collation where `Charset` = 'utf8' and `Collation` = 'utf8_bin'",
	}, {
		input: "show create database d",
	}, {
		input: "show create event e",
	}, {
		input: "show create function f",
	}, {
		input: "show create procedure p",
	}, {
		input: "show create table t",
	}, {
		input: "show create trigger t",
	}, {
		input:  "show create user u",
		output: "show create user",
	}, {
		input: "show create view v",
	}, {
		input:  "show databases",
		output: "show databases",
	}, {
		input:  "show databases like '%'",
		output: "show databases like '%'",
	}, {
		input:  "show schemas",
		output: "show databases",
	}, {
		input:  "show schemas like '%'",
		output: "show databases like '%'",
	}, {
		input:  "show engine INNODB",
		output: "show engine",
	}, {
		input: "show engines",
	}, {
		input:  "show storage engines",
		output: "show storage",
	}, {
		input: "show errors",
	}, {
		input: "show events",
	}, {
		input: "show function code func",
	}, {
		input: "show function status",
	}, {
		input:  "show grants for 'root@localhost'",
		output: "show grants",
	}, {
		input:  "show index from t",
		output: "show indexes from t",
	}, {
		input: "show indexes from t",
	}, {
		input:  "show keys from t",
		output: "show indexes from t",
	}, {
		input:  "show master status",
		output: "show master",
	}, {
		input: "show open tables",
	}, {
		input:  "show plugins",
		output: "show plugins",
	}, {
		input:  "show privileges",
		output: "show privileges",
	}, {
		input: "show procedure code p",
	}, {
		input: "show procedure status",
	}, {
		input:  "show processlist",
		output: "show processlist",
	}, {
		input:  "show full processlist",
		output: "show processlist",
	}, {
		input:  "show profile cpu for query 1",
		output: "show profile",
	}, {
		input:  "show profiles",
		output: "show profiles",
	}, {
		input:  "show relaylog events",
		output: "show relaylog",
	}, {
		input:  "show slave hosts",
		output: "show slave",
	}, {
		input:  "show slave status",
		output: "show slave",
	}, {
		input:  "show status",
		output: "show status",
	}, {
		input:  "show global status",
		output: "show global status",
	}, {
		input:  "show session status",
		output: "show status",
	}, {
		input: "show table status",
	}, {
		input: "show table status from dbname",
	}, {
		input:  "show table status in dbname",
		output: "show table status from dbname",
	}, {
		input:  "show table status in dbname LIKE '%' ",
		output: "show table status from dbname like '%'",
	}, {
		input:  "show table status from dbname Where col=42 ",
		output: "show table status from dbname where col = 42",
	}, {
		input: "show tables",
	}, {
		input: "show tables like '%keyspace%'",
	}, {
		input: "show tables where 1 = 0",
	}, {
		input: "show tables from a",
	}, {
		input: "show tables from a where 1 = 0",
	}, {
		input: "show tables from a like '%keyspace%'",
	}, {
		input: "show full tables",
	}, {
		input: "show full tables from a",
	}, {
		input:  "show full tables in a",
		output: "show full tables from a",
	}, {
		input: "show full tables from a like '%keyspace%'",
	}, {
		input: "show full tables from a where 1 = 0",
	}, {
		input: "show full tables like '%keyspace%'",
	}, {
		input: "show full tables where 1 = 0",
	}, {
		input:  "show full columns in a in b like '%'",
		output: "show full columns from a from b like '%'",
	}, {
		input: "show full columns from messages from test_keyspace like '%'",
	}, {
		input:  "show full fields from a like '%'",
		output: "show full columns from a like '%'",
	}, {
		input:  "show fields from a where 1 = 1",
		output: "show columns from a where 1 = 1",
	}, {
		input:  "show triggers",
		output: "show triggers",
	}, {
		input:  "show variables",
		output: "show variables",
	}, {
		input:  "show global variables",
		output: "show global variables",
	}, {
		input:  "show session variables",
		output: "show variables",
	}, {
		input: "show global vgtid_executed",
	}, {
		input: "show global vgtid_executed from ks",
	}, {
		input: "show global gtid_executed",
	}, {
		input: "show global gtid_executed from ks",
	}, {
		input:  "show vitess_keyspaces",
		output: "show keyspaces",
	}, {
		input:  "show vitess_keyspaces like '%'",
		output: "show keyspaces like '%'",
	}, {
		input: "show vitess_metadata variables",
	}, {
		input: "show vitess_replication_status",
	}, {
		input: "show vitess_replication_status like '%'",
	}, {
		input: "show vitess_shards",
	}, {
		input: "show vitess_shards like '%'",
	}, {
		input: "show vitess_tablets",
	}, {
		input: "show vitess_tablets like '%'",
	}, {
		input: "show vitess_tablets where hostname = 'some-tablet'",
	}, {
		input: "show vitess_targets",
	}, {
		input: "show vschema tables",
	}, {
		input: "show vschema vindexes",
	}, {
		input: "show vschema vindexes from t",
	}, {
		input:  "show vschema vindexes on t",
		output: "show vschema vindexes from t",
	}, {
		input: "show vitess_migrations",
	}, {
		input: "show vitess_migrations from ks",
	}, {
		input: "show vitess_migrations from ks where col = 42",
	}, {
		input: `show vitess_migrations from ks like '%pattern'`,
	}, {
		input: "show vitess_migrations like '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90'",
	}, {
		input: "show vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' logs",
	}, {
		input: "revert vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90'",
	}, {
		input: "revert /*vt+ uuid=123 */ vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90'",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' retry",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' cleanup",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' launch",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' launch vitess_shards '-40'",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' launch vitess_shards '-40,40-80'",
	}, {
		input: "alter vitess_migration launch all",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' complete",
	}, {
		input: "alter vitess_migration complete all",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' cancel",
	}, {
		input: "alter vitess_migration cancel all",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' throttle",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' throttle expire '1h'",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' throttle ratio 0.7",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' throttle expire '1h' ratio 0.7",
	}, {
		input: "alter vitess_migration '9748c3b7_7fdb_11eb_ac2c_f875a4d24e90' unthrottle",
	}, {
		input: "alter vitess_migration throttle all",
	}, {
		input: "alter vitess_migration unthrottle all",
	}, {
		input: "alter vitess_migration throttle all expire '1h'",
	}, {
		input: "alter vitess_migration throttle all ratio 0.7",
	}, {
		input: "alter vitess_migration throttle all expire '1h' ratio 0.7",
	}, {
		input: "show vitess_throttled_apps",
	}, {
		input: "show vitess_throttler status",
	}, {
		input: "show warnings",
	}, {
		input:  "select warnings from t",
		output: "select `warnings` from t",
	}, {
		input:  "show foobar",
		output: "show foobar",
	}, {
		input:  "show foobar like select * from table where syntax is 'ignored'",
		output: "show foobar",
	}, {
		input:  "use db",
		output: "use db",
	}, {
		input:  "use duplicate",
		output: "use `duplicate`",
	}, {
		input:  "use `ks:-80@master`",
		output: "use `ks:-80@master`",
	}, {
		input:  "use `ks:-80@primary`",
		output: "use `ks:-80@primary`",
	}, {
		input:  "use @replica",
		output: "use `@replica`",
	}, {
		input:  "use ks@replica",
		output: "use `ks@replica`",
	}, {
		input:  "describe select * from t",
		output: "explain select * from t",
	}, {
		input:  "describe /*vt+ execute_dml_queries */ select * from t",
		output: "explain /*vt+ execute_dml_queries */ select * from t",
	}, {
		input:  "desc select * from t",
		output: "explain select * from t",
	}, {
		input:  "desc foobar",
		output: "explain foobar",
	}, {
		input: "explain t1",
	}, {
		input: "explain t1 col",
	}, {
		input: "explain t1 '%col%'",
	}, {
		input: "explain select * from t",
	}, {
		input: "explain format = traditional select * from t",
	}, {
		input: "vexplain queries select * from t",
	}, {
		input: "vexplain all select * from t",
	}, {
		input: "vexplain plan select * from t",
	}, {
		input:  "vexplain select * from t",
		output: "vexplain plan select * from t",
	}, {
		input: "explain analyze select * from t",
	}, {
		input: "explain format = tree select * from t",
	}, {
		input: "explain format = json select * from t",
	}, {
		input: "explain format = vtexplain select * from t",
	}, {
		input: "explain format = vitess select * from t",
	}, {
		input:  "describe format = vitess select * from t",
		output: "explain format = vitess select * from t",
	}, {
		input:  "describe format = vtexplain select * from t",
		output: "explain format = vtexplain select * from t",
	}, {
		input: "explain delete from t",
	}, {
		input: "explain insert into t(col1, col2) values (1, 2)",
	}, {
		input: "explain update t set col = 2",
	}, {
		input:  "truncate table foo",
		output: "truncate table foo",
	}, {
		input:  "truncate foo",
		output: "truncate table foo",
	}, {
		input:  "repair foo",
		output: "otheradmin",
	}, {
		input:  "optimize foo",
		output: "otheradmin",
	}, {
		input:  "lock tables foo read",
		output: "lock tables foo read",
	}, {
		input:  "lock tables foo write",
		output: "lock tables foo write",
	}, {
		input:  "lock tables foo read local",
		output: "lock tables foo read local",
	}, {
		input:  "lock tables foo low_priority write",
		output: "lock tables foo low_priority write",
	}, {
		input:  "unlock tables",
		output: "unlock tables",
	}, {
		input: "select /* EQ true */ 1 from t where a = true",
	}, {
		input: "select /* EQ false */ 1 from t where a = false",
	}, {
		input: "select /* NE true */ 1 from t where a != true",
	}, {
		input: "select /* NE false */ 1 from t where a != false",
	}, {
		input: "select /* LT true */ 1 from t where a < true",
	}, {
		input: "select /* LT false */ 1 from t where a < false",
	}, {
		input: "select /* GT true */ 1 from t where a > true",
	}, {
		input: "select /* GT false */ 1 from t where a > false",
	}, {
		input: "select /* LE true */ 1 from t where a <= true",
	}, {
		input: "select /* LE false */ 1 from t where a <= false",
	}, {
		input: "select /* GE true */ 1 from t where a >= true",
	}, {
		input: "select /* GE false */ 1 from t where a >= false",
	}, {
		input:  "select * from t order by a collate utf8_general_ci",
		output: "select * from t order by a collate utf8_general_ci asc",
	}, {
		input: "select k collate latin1_german2_ci as k1 from t1 order by k1 asc",
	}, {
		input: "select * from t group by a collate utf8_general_ci",
	}, {
		input:  "select MAX(k collate latin1_german2_ci) from t1",
		output: "select max(k collate latin1_german2_ci) from t1",
	}, {
		input: "select distinct k collate latin1_german2_ci from t1",
	}, {
		input: "select * from t1 where 'Müller' collate latin1_german2_ci = k",
	}, {
		input: "select * from t1 where k like 'Müller' collate latin1_german2_ci",
	}, {
		input: "select k from t1 group by k having k = 'Müller' collate latin1_german2_ci",
	}, {
		input: "select k from t1 join t2 order by a collate latin1_german2_ci asc, b collate latin1_german2_ci asc",
	}, {
		input:  "select k collate 'latin1_german2_ci' as k1 from t1 order by k1 asc",
		output: "select k collate latin1_german2_ci as k1 from t1 order by k1 asc",
	}, {
		input:  "select /* drop trailing semicolon */ 1 from dual;",
		output: "select /* drop trailing semicolon */ 1 from dual",
	}, {
		input: "select /* cache directive */ sql_no_cache 'foo' from t",
	}, {
		input: "select distinct sql_no_cache 'foo' from t",
	}, {
		input:  "select sql_no_cache distinct 'foo' from t",
		output: "select distinct sql_no_cache 'foo' from t",
	}, {
		input:  "select sql_no_cache straight_join distinct 'foo' from t",
		output: "select distinct sql_no_cache straight_join 'foo' from t",
	}, {
		input:  "select straight_join distinct sql_no_cache 'foo' from t",
		output: "select distinct sql_no_cache straight_join 'foo' from t",
	}, {
		input:  "select sql_calc_found_rows 'foo' from t",
		output: "select sql_calc_found_rows 'foo' from t",
	}, {
		input:  "select binary 'a' = 'A' from t",
		output: "select convert('a', binary) = 'A' from t",
	}, {
		input: "select 1 from t where foo = _binary 'bar'",
	}, {
		input: "select 1 from t where foo = _utf8 'bar' and bar = _latin1 'sjösjuk'",
	}, {
		input:  "select 1 from t where foo = _binary'bar'",
		output: "select 1 from t where foo = _binary 'bar'",
	}, {
		input: "select 1 from t where foo = _utf8mb4 'bar'",
	}, {
		input:  "select 1 from t where foo = _utf8mb4'bar'",
		output: "select 1 from t where foo = _utf8mb4 'bar'",
	}, {
		input:  "select 1 from t where foo = _utf8mb3 'bar'",
		output: "select 1 from t where foo = _utf8 'bar'",
	}, {
		input:  "select 1 from t where foo = _utf8mb3'bar'",
		output: "select 1 from t where foo = _utf8 'bar'",
	}, {
		input: "select match(a) against ('foo') from t",
	}, {
		input: "select match(a1, a2) against ('foo' in natural language mode with query expansion) from t",
	}, {
		input:  "select database()",
		output: "select database() from dual",
	}, {
		input:  "select schema()",
		output: "select schema() from dual",
	}, {
		input: "select title from video as v where match(v.title, v.tag) against ('DEMO' in boolean mode)",
	}, {
		input:  "SELECT id FROM blog_posts USE INDEX (PRIMARY) WHERE id = 10",
		output: "select id from blog_posts use index (`PRIMARY`) where id = 10",
	}, {
		input:  "select name, group_concat(score) from t group by name",
		output: "select `name`, group_concat(score) from t group by `name`",
	}, {
		input:  "select name, group_concat(distinct id, score order by id desc separator ':') from t group by name",
		output: "select `name`, group_concat(distinct id, score order by id desc separator ':') from t group by `name`",
	}, {
		input:  "select name, group_concat(distinct id, score order by id desc separator ':' limit 1) from t group by name",
		output: "select `name`, group_concat(distinct id, score order by id desc separator ':' limit 1) from t group by `name`",
	}, {
		input:  "select name, group_concat(distinct id, score order by id desc separator ':' limit 10, 2) from t group by name",
		output: "select `name`, group_concat(distinct id, score order by id desc separator ':' limit 10, 2) from t group by `name`",
	}, {
		input: "select * from t partition (p0)",
	}, {
		input: "select * from t partition (p0, p1)",
	}, {
		input: "select e.id, s.city from employees as e join stores partition (p1) as s on e.store_id = s.id",
	}, {
		input: "select truncate(120.3333, 2) from dual",
	}, {
		input: "update t partition (p0) set a = 1",
	}, {
		input: "insert into t partition (p0) values (1, 'asdf')",
	}, {
		input: "insert into t1 select * from t2 partition (p0)",
	}, {
		input: "replace into t partition (p0) values (1, 'asdf')",
	}, {
		input: "delete from t partition (p0) where a = 1",
	}, {
		input: "stream * from t",
	}, {
		input: "stream /* comment */ * from t",
	}, {
		input: "vstream * from t",
	}, {
		input: "begin",
	}, {
		input:  "begin;",
		output: "begin",
	}, {
		input:  "start transaction",
		output: "begin",
	}, {
		input: "start transaction with consistent snapshot",
	}, {
		input: "start transaction read write",
	}, {
		input: "start transaction read only",
	}, {
		input: "start transaction read only, with consistent snapshot",
	}, {
		input: "commit",
	}, {
		input: "rollback",
	}, {
		input: "create database /* simple */ test_db",
	}, {
		input:  "create schema test_db",
		output: "create database test_db",
	}, {
		input: "create database /* simple */ if not exists test_db",
	}, {
		input:  "create schema if not exists test_db",
		output: "create database if not exists test_db",
	}, {
		input: "create database test_db default collate 'utf8mb4_general_ci' collate utf8mb4_general_ci",
	}, {
		input: "create database test_db character set geostd8",
	}, {
		input:      "alter table corder zzzz zzzz zzzz",
		output:     "alter table corder",
		partialDDL: true,
	}, {
		input:      "create database test_db character set * unparsable",
		output:     "create database test_db",
		partialDDL: true,
	}, {
		input:  "CREATE DATABASE IF NOT EXISTS `mysql` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci ENCRYPTION='N';",
		output: "create database if not exists mysql default character set utf8mb4 collate utf8mb4_0900_ai_ci encryption 'N'",
	}, {
		input: "drop /* simple */ database test_db",
	}, {
		input:  "drop schema test_db",
		output: "drop database test_db",
	}, {
		input: "drop /* simple */ database if exists test_db",
	}, {
		input:  "delete a.*, b.* from tbl_a a, tbl_b b where a.id = b.id and b.name = 'test'",
		output: "delete a, b from tbl_a as a, tbl_b as b where a.id = b.id and b.`name` = 'test'",
	}, {
		input:  "select distinctrow a.* from (select (1) from dual union all select 1 from dual) a",
		output: "select distinct a.* from (select 1 from dual union all select 1 from dual) as a",
	}, {
		input: "select `weird function name`() from t",
	}, {
		input:  "select all* from t",
		output: "select * from t",
	}, {
		input:  "select distinct* from t",
		output: "select distinct * from t",
	}, {
		input: "select status() from t", // should not escape function names that are keywords
	}, {
		input: "select * from `weird table name`",
	}, {
		input:  "SHOW FULL TABLES FROM `jiradb` LIKE 'AO_E8B6CC_ISSUE_MAPPING'",
		output: "show full tables from jiradb like 'AO_E8B6CC_ISSUE_MAPPING'",
	}, {
		input:  "SHOW FULL COLUMNS FROM AO_E8B6CC_ISSUE_MAPPING FROM jiradb LIKE '%'",
		output: "show full columns from AO_E8B6CC_ISSUE_MAPPING from jiradb like '%'",
	}, {
		input:  "SHOW KEYS FROM `AO_E8B6CC_ISSUE_MAPPING` FROM `jiradb`",
		output: "show indexes from AO_E8B6CC_ISSUE_MAPPING from jiradb",
	}, {
		input:  "SHOW CREATE TABLE `jiradb`.`AO_E8B6CC_ISSUE_MAPPING`",
		output: "show create table jiradb.AO_E8B6CC_ISSUE_MAPPING",
	}, {
		input:  "SHOW INDEX FROM `AO_E8B6CC_ISSUE_MAPPING` FROM `jiradb`",
		output: "show indexes from AO_E8B6CC_ISSUE_MAPPING from jiradb",
	}, {
		input:  "SHOW FULL TABLES FROM `jiradb` LIKE '%'",
		output: "show full tables from jiradb like '%'",
	}, {
		input:  "SHOW EXTENDED INDEX FROM `AO_E8B6CC_PROJECT_MAPPING` FROM `jiradb`",
		output: "show indexes from AO_E8B6CC_PROJECT_MAPPING from jiradb",
	}, {
		input:  "SHOW EXTENDED KEYS FROM `AO_E8B6CC_ISSUE_MAPPING` FROM `jiradb`",
		output: "show indexes from AO_E8B6CC_ISSUE_MAPPING from jiradb",
	}, {
		input:  "SHOW CREATE TABLE `jiradb`.`AO_E8B6CC_ISSUE_MAPPING`",
		output: "show create table jiradb.AO_E8B6CC_ISSUE_MAPPING",
	}, {
		input: "create table t1 ( check (c1 <> c2), c1 int check (c1 > 10), c2 int constraint c2_positive check (c2 > 0), c3 int check (c3 < 100), constraint c1_nonzero check (c1 <> 0), check (c1 > c3))",
		output: "create table t1 (\n" +
			"\tc1 int,\n" +
			"\tc2 int,\n" +
			"\tc3 int,\n" +
			"\tcheck (c1 != c2),\n" +
			"\tcheck (c1 > 10),\n" +
			"\tconstraint c2_positive check (c2 > 0),\n" +
			"\tcheck (c3 < 100),\n" +
			"\tconstraint c1_nonzero check (c1 != 0),\n" +
			"\tcheck (c1 > c3)\n)",
	}, {
		input:  "SHOW INDEXES FROM `AO_E8B6CC_ISSUE_MAPPING` FROM `jiradb`",
		output: "show indexes from AO_E8B6CC_ISSUE_MAPPING from jiradb",
	}, {
		input:  "SHOW FULL TABLES FROM `jiradb` LIKE '%'",
		output: "show full tables from jiradb like '%'",
	}, {
		input:  "SHOW EXTENDED INDEXES FROM `AO_E8B6CC_PROJECT_MAPPING` FROM `jiradb`",
		output: "show indexes from AO_E8B6CC_PROJECT_MAPPING from jiradb",
	}, {
		input:  "SHOW EXTENDED INDEXES IN `AO_E8B6CC_PROJECT_MAPPING` IN `jiradb`",
		output: "show indexes from AO_E8B6CC_PROJECT_MAPPING from jiradb",
	}, {
		input:  "do 1",
		output: "otheradmin",
	}, {
		input:  "do funcCall(), 2 = 1, 3 + 1",
		output: "otheradmin",
	}, {
		input: "savepoint a",
	}, {
		input: "savepoint `@@@;a`",
	}, {
		input: "rollback to a",
	}, {
		input: "rollback to `@@@;a`",
	}, {
		input:  "rollback work to a",
		output: "rollback to a",
	}, {
		input:  "rollback to savepoint a",
		output: "rollback to a",
	}, {
		input:  "rollback work to savepoint a",
		output: "rollback to a",
	}, {
		input: "release savepoint a",
	}, {
		input: "release savepoint `@@@;a`",
	}, {
		input: "call proc()",
	}, {
		input: "call qualified.proc()",
	}, {
		input: "call proc(1, 'foo')",
	}, {
		input: "call proc(@param)",
	}, {
		input:  "PREPARE stmt1 FROM 'SELECT SQRT(POW(?,2) + POW(?,2)) AS hypotenuse'",
		output: "prepare stmt1 from 'SELECT SQRT(POW(?,2) + POW(?,2)) AS hypotenuse'",
	}, {
		input:  "PREPARE stmt2 FROM @s",
		output: "prepare stmt2 from @s",
	}, {
		input:  "PREPARE /* comment */ stmt2 FROM @s",
		output: "prepare /* comment */ stmt2 from @s",
	}, {
		input:  "EXECUTE stmt1",
		output: "execute stmt1",
	}, {
		input:  "EXECUTE /* comment */ stmt1",
		output: "execute /* comment */ stmt1",
	}, {
		input:  "EXECUTE stmt1 USING @a",
		output: "execute stmt1 using @a",
	}, {
		input:  "EXECUTE stmt1 USING @a, @b",
		output: "execute stmt1 using @a, @b",
	}, {
		input:  "DEALLOCATE PREPARE stmt1",
		output: "deallocate prepare stmt1",
	}, {
		input:  "DROP PREPARE stmt1",
		output: "deallocate prepare stmt1",
	}, {
		input:  "DROP /* comment */ PREPARE stmt1",
		output: "deallocate /* comment */ prepare stmt1",
	}, {
		input:  `SELECT JSON_PRETTY('{"a":"10","b":"15","x":"25"}')`,
		output: `select json_pretty('{\"a\":\"10\",\"b\":\"15\",\"x\":\"25\"}') from dual`,
	}, {
		input:  `SELECT JSON_PRETTY(N'{"a":"10","b":"15","x":"25"}')`,
		output: `select json_pretty(N'{\"a\":\"10\",\"b\":\"15\",\"x\":\"25\"}') from dual`,
		/*We need to ignore this test because, after the normalizer, we change the produced NChar
		string into an introducer expression, so the vttablet will never see a NChar string */
		ignoreNormalizerTest: true,
	}, {
		input:  "SELECT jcol, JSON_PRETTY(jcol) from jtable",
		output: "select jcol, json_pretty(jcol) from jtable",
	}, {
		input:  "SELECT JSON_PRETTY(@j)",
		output: "select json_pretty(@j) from dual",
	}, {
		input:  "SELECT jcol, JSON_STORAGE_SIZE(jcol) AS Size FROM jtable",
		output: "select jcol, json_storage_size(jcol) as Size from jtable",
	}, {
		input:  `SELECT jcol, JSON_STORAGE_SIZE(N'{"a":"10","b":"15","x":"25"}') AS Size FROM jtable`,
		output: `select jcol, json_storage_size(N'{\"a\":\"10\",\"b\":\"15\",\"x\":\"25\"}') as Size from jtable`,
		/*We need to ignore this test because, after the normalizer, we change the produced NChar
		string into an introducer expression, so the vttablet will never see a NChar string */
		ignoreNormalizerTest: true,
	}, {
		input:  `SELECT JSON_STORAGE_SIZE('[100, "sakila", [1, 3, 5], 425.05]') AS A, JSON_STORAGE_SIZE('{"a": 1000, "b": "a", "c": "[1, 3, 5, 7]"}') AS B, JSON_STORAGE_SIZE('{"a": 1000, "b": "wxyz", "c": "[1, 3, 5, 7]"}') AS C,JSON_STORAGE_SIZE('[100, "json", [[10, 20, 30], 3, 5], 425.05]') AS D`,
		output: `select json_storage_size('[100, \"sakila\", [1, 3, 5], 425.05]') as A, json_storage_size('{\"a\": 1000, \"b\": \"a\", \"c\": \"[1, 3, 5, 7]\"}') as B, json_storage_size('{\"a\": 1000, \"b\": \"wxyz\", \"c\": \"[1, 3, 5, 7]\"}') as C, json_storage_size('[100, \"json\", [[10, 20, 30], 3, 5], 425.05]') as D from dual`,
	}, {
		input:  "SELECT JSON_STORAGE_SIZE(@j)",
		output: "select json_storage_size(@j) from dual",
	}, {
		input:  "SELECT JSON_STORAGE_FREE(jcol) FROM jtable",
		output: "select json_storage_free(jcol) from jtable",
	}, {
		input:  `SELECT JSON_STORAGE_FREE('{"a":"10","b":"15","x":"25"}')`,
		output: `select json_storage_free('{\"a\":\"10\",\"b\":\"15\",\"x\":\"25\"}') from dual`,
	}, {
		input:  `SELECT JSON_STORAGE_FREE(N'{"a":"10","b":"15","x":"25"}')`,
		output: `select json_storage_free(N'{\"a\":\"10\",\"b\":\"15\",\"x\":\"25\"}') from dual`,
		/*We need to ignore this test because, after the normalizer, we change the produced NChar
		string into an introducer expression, so the vttablet will never see a NChar string */
		ignoreNormalizerTest: true,
	}, {
		input:  "SELECT JSON_STORAGE_FREE(@j)",
		output: "select json_storage_free(@j) from dual",
	}, {
		input:  "SELECT LTRIM('abc')",
		output: "select ltrim('abc') from dual",
	}, {
		input:  "SELECT RTRIM('abc')",
		output: "select rtrim('abc') from dual",
	}, {
		input:  "SELECT TRIM('  abc  ')",
		output: "select trim('  abc  ') from dual",
	}, {
		input:  "SELECT TRIM('aa' FROM 'aabccaaa')",
		output: "select trim('aa' from 'aabccaaa') from dual",
	}, {
		input:  "SELECT TRIM(LEADING FROM 'aabccaaa')",
		output: "select trim(leading from 'aabccaaa') from dual",
	}, {
		input:  "SELECT TRIM(TRAILING FROM 'abca')",
		output: "select trim(trailing from 'abca') from dual",
	}, {
		input:  "SELECT TRIM(BOTH FROM 'abc')",
		output: "select trim(both from 'abc') from dual",
	}, {
		input:  "SELECT TRIM(LEADING 'a' FROM 'abc')",
		output: "select trim(leading 'a' from 'abc') from dual",
	}, {
		input:  "SELECT TRIM(TRAILING 'a' FROM 'abc')",
		output: "select trim(trailing 'a' from 'abc') from dual",
	}, {
		input:  "SELECT TRIM(BOTH 'a' FROM 'abc')",
		output: "select trim(both 'a' from 'abc') from dual",
	}, {
		input: `SELECT * FROM JSON_TABLE('[ {"c1": null} ]','$[*]' COLUMNS( c1 INT PATH '$.c1' ERROR ON ERROR )) as jt`,
		output: `select * from json_table('[ {\"c1\": null} ]', '$[*]' columns(
	c1 INT path '$.c1' error on error 
	)
) as jt`,
	}, {
		input: `SELECT * FROM  JSON_TABLE(    '[{"a": 1, "b": [11,111]}, {"a": 2, "b": [22,222]}]', '$[*]' COLUMNS(a INT PATH '$.a', NESTED PATH '$.b[*]' COLUMNS (b1 INT PATH '$'), NESTED PATH '$.b[*]' COLUMNS (b2 INT PATH '$'))) AS jt`,
		output: `select * from json_table('[{\"a\": 1, \"b\": [11,111]}, {\"a\": 2, \"b\": [22,222]}]', '$[*]' columns(
	a INT path '$.a' ,
	nested path '$.b[*]' columns(
	b1 INT path '$' 
),
	nested path '$.b[*]' columns(
	b2 INT path '$' 
)
	)
) as jt`,
	}, {
		input: `SELECT * FROM JSON_TABLE('[ {"c1": null} ]','$[*]' COLUMNS( c1 INT PATH '$.c1' ERROR ON ERROR )) as jt`,
		output: `select * from json_table('[ {\"c1\": null} ]', '$[*]' columns(
	c1 INT path '$.c1' error on error 
	)
) as jt`,
	}, {
		input: `SELECT * FROM JSON_TABLE('[{"a":"3"},{"a":2},{"b":1},{"a":0},{"a":[1,2]}]', "$[*]" COLUMNS(rowid FOR ORDINALITY, ac VARCHAR(100) PATH "$.a" DEFAULT '111' ON EMPTY DEFAULT '999' ON ERROR,  aj JSON PATH "$.a" DEFAULT '{"x": 333}' ON EMPTY, bx INT EXISTS PATH "$.b" ) ) AS tt`,
		output: `select * from json_table('[{\"a\":\"3\"},{\"a\":2},{\"b\":1},{\"a\":0},{\"a\":[1,2]}]', '$[*]' columns(
	rowid for ordinality,
	ac VARCHAR(100) path '$.a' default '111' on empty default '999' on error ,
	aj JSON path '$.a' default '{\"x\": 333}' on empty ,
	bx INT exists path '$.b' 
	)
) as tt`,
	}, {
		input: `SELECT * FROM  JSON_TABLE(    '[ {"a": 1, "b": [11,111]}, {"a": 2, "b": [22,222]}, {"a":3}]',    '$[*]' COLUMNS(            a INT PATH '$.a',            NESTED PATH '$.b[*]' COLUMNS (b INT PATH '$')           )   ) AS jt WHERE b IS NOT NULL`,
		output: `select * from json_table('[ {\"a\": 1, \"b\": [11,111]}, {\"a\": 2, \"b\": [22,222]}, {\"a\":3}]', '$[*]' columns(
	a INT path '$.a' ,
	nested path '$.b[*]' columns(
	b INT path '$' 
)
	)
) as jt where b is not null`,
	}, {
		input: `SELECT * FROM  JSON_TABLE(    '[{"x":2,"y":"8"},{"x":"3","y":"7"},{"x":"4","y":6}]',    "$[1]" COLUMNS(      xval VARCHAR(100) PATH "$.x",      yval VARCHAR(100) PATH "$.y"    )  ) AS  jt1`,
		output: `select * from json_table('[{\"x\":2,\"y\":\"8\"},{\"x\":\"3\",\"y\":\"7\"},{\"x\":\"4\",\"y\":6}]', '$[1]' columns(
	xval VARCHAR(100) path '$.x' ,
	yval VARCHAR(100) path '$.y' 
	)
) as jt1`,
	}, {
		input: `SELECT * FROM  JSON_TABLE(    '[{"a": "a_val","b": [{"c": "c_val", "l": [1,2]}]},{"a": "a_val", "b": [{"c": "c_val","l": [11]}, {"c": "c_val", "l": [22]}]}]',    '$[*]' COLUMNS(      top_ord FOR ORDINALITY,      apath VARCHAR(10) PATH '$.a',      NESTED PATH '$.b[*]' COLUMNS (        bpath VARCHAR(10) PATH '$.c',        ord FOR ORDINALITY,        NESTED PATH '$.l[*]' COLUMNS (lpath varchar(10) PATH '$')        )    )) as jt`,
		output: `select * from json_table('[{\"a\": \"a_val\",\"b\": [{\"c\": \"c_val\", \"l\": [1,2]}]},{\"a\": \"a_val\", \"b\": [{\"c\": \"c_val\",\"l\": [11]}, {\"c\": \"c_val\", \"l\": [22]}]}]', '$[*]' columns(
	top_ord for ordinality,
	apath VARCHAR(10) path '$.a' ,
	nested path '$.b[*]' columns(
	bpath VARCHAR(10) path '$.c' ,
	ord for ordinality,
	nested path '$.l[*]' columns(
	lpath varchar(10) path '$' 
)
)
	)
) as jt`,
	}, {
		input: `SELECT * FROM JSON_TABLE('[{"x":2,"y":"8"},{"x":"3","y":"7"},{"x":"4","y":6}]', "$[1]" COLUMNS( xval VARCHAR(100) PATH "$.x", yval VARCHAR(100) PATH "$.y")) AS  jt1;`,
		output: `select * from json_table('[{\"x\":2,\"y\":\"8\"},{\"x\":\"3\",\"y\":\"7\"},{\"x\":\"4\",\"y\":6}]', '$[1]' columns(
	xval VARCHAR(100) path '$.x' ,
	yval VARCHAR(100) path '$.y' 
	)
) as jt1`,
	}, {
		input:  "SELECT JSON_ARRAY()",
		output: "select json_array() from dual",
	}, {
		input:  "SELECT JSON_ARRAY(1)",
		output: "select json_array(1) from dual",
	}, {
		input:  "SELECT JSON_ARRAY('abc')",
		output: "select json_array('abc') from dual",
	}, {
		input:  "SELECT JSON_ARRAY(BIN(11))",
		output: "select json_array(BIN(11)) from dual",
	}, {
		input:  `SELECT JSON_ARRAY(1, "abc", NULL, TRUE, CURTIME());`,
		output: `select json_array(1, 'abc', null, true, curtime()) from dual`,
	}, {
		input:  "SELECT JSON_OBJECT(1,2)",
		output: "select json_object(1, 2) from dual",
	}, {
		input:  "SELECT JSON_OBJECT(1,'abc')",
		output: "select json_object(1, 'abc') from dual",
	}, {
		input:  "SELECT JSON_OBJECT('abc',1)",
		output: "select json_object('abc', 1) from dual",
	}, {
		input:  "SELECT JSON_OBJECT(BIN(1),2)",
		output: "select json_object(BIN(1), 2) from dual",
	}, {
		input:  "SELECT JSON_OBJECT(BIN(1),2,'abc',ASCII(4))",
		output: "select json_object(BIN(1), 2, 'abc', ASCII(4)) from dual",
	}, {
		input:  "SELECT JSON_QUOTE(BIN(11))",
		output: "select json_quote(BIN(11)) from dual",
	}, {
		input:  `SELECT JSON_QUOTE('null'), JSON_QUOTE('"null"')`,
		output: `select json_quote('null'), json_quote('\"null\"') from dual`,
	}, {
		input:  "select t1.a, dt.a from t1, lateral (select t1.a+t2.a as a from t2) dt",
		output: "select t1.a, dt.a from t1, lateral (select t1.a + t2.a as a from t2) as dt",
	}, {
		input:  "select b from v1 vq1, lateral (select count(*) from v1 vq2 having vq1.b = 3) dt",
		output: "select b from v1 as vq1, lateral (select count(*) from v1 as vq2 having vq1.b = 3) as dt",
	}, {
		input:  `SELECT JSON_SCHEMA_VALID('{"type":"string","pattern":"("}', '"abc"')`,
		output: `select json_schema_valid('{\"type\":\"string\",\"pattern\":\"(\"}', '\"abc\"') from dual`,
	}, {
		input:  `SELECT JSON_SCHEMA_VALID('{"type":"string","pattern":"("}', @a)`,
		output: `select json_schema_valid('{\"type\":\"string\",\"pattern\":\"(\"}', @a) from dual`,
	}, {
		input:  `SELECT JSON_SCHEMA_VALID(@b, BIN(1))`,
		output: `select json_schema_valid(@b, BIN(1)) from dual`,
	}, {
		input:  `SELECT JSON_SCHEMA_VALID(N'{"type":"string","pattern":"("}', '"abc"')`,
		output: `select json_schema_valid(N'{\"type\":\"string\",\"pattern\":\"(\"}', '\"abc\"') from dual`,
		/*We need to ignore this test because, after the normalizer, we change the produced NChar
		string into an introducer expression, so the vttablet will never see a NChar string */
		ignoreNormalizerTest: true,
	}, {
		input:  `SELECT JSON_SCHEMA_VALIDATION_REPORT('{"type":"string","pattern":"("}', '"abc"')`,
		output: `select json_schema_validation_report('{\"type\":\"string\",\"pattern\":\"(\"}', '\"abc\"') from dual`,
	}, {
		input:  `SELECT JSON_SCHEMA_VALIDATION_REPORT('{"type":"string","pattern":"("}', @a)`,
		output: `select json_schema_validation_report('{\"type\":\"string\",\"pattern\":\"(\"}', @a) from dual`,
	}, {
		input:  `SELECT JSON_SCHEMA_VALIDATION_REPORT(@b, BIN(1))`,
		output: `select json_schema_validation_report(@b, BIN(1)) from dual`,
	}, {
		input:  `SELECT JSON_SCHEMA_VALIDATION_REPORT(N'{"type":"string","pattern":"("}', '"abc"')`,
		output: `select json_schema_validation_report(N'{\"type\":\"string\",\"pattern\":\"(\"}', '\"abc\"') from dual`,
		/*We need to ignore this test because, after the normalizer, we change the produced NChar
		string into an introducer expression, so the vttablet will never see a NChar string */
		ignoreNormalizerTest: true,
	}, {
		input:  `SELECT JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '1')`,
		output: `select json_contains('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '1') from dual`,
	}, {
		input:  "SELECT JSON_CONTAINS(@j, @j2)",
		output: "select json_contains(@j, @j2) from dual",
	}, {
		input:  "SELECT JSON_CONTAINS(@j, @j2,'$.a', @j)",
		output: "select json_contains(@j, @j2, '$.a', @j) from dual",
	}, {
		input:  "SELECT JSON_CONTAINS_PATH(@j, 'one', '$.a', '$.e')",
		output: "select json_contains_path(@j, 'one', '$.a', '$.e') from dual",
	}, {
		input:  `SELECT JSON_CONTAINS_PATH('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a', '$.e')`,
		output: `select json_contains_path('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', 'one', '$.a', '$.e') from dual`,
	}, {
		input:  "SELECT JSON_CONTAINS_PATH(@j, TRIM('one'), '$.a', '$.e')",
		output: "select json_contains_path(@j, trim('one'), '$.a', '$.e') from dual",
	}, {
		input:  "SELECT JSON_CONTAINS_PATH(@j, @k, '$.a', @i)",
		output: "select json_contains_path(@j, @k, '$.a', @i) from dual",
	}, {
		input:  "SELECT JSON_EXTRACT(@j, '$.a')",
		output: "select json_extract(@j, '$.a') from dual",
	}, {
		input:  `SELECT c, JSON_EXTRACT(c, "$.id"), g FROM jemp WHERE JSON_EXTRACT(c, "$.id") > 1 ORDER BY JSON_EXTRACT(c, "$.name")`,
		output: "select c, json_extract(c, '$.id'), g from jemp where json_extract(c, '$.id') > 1 order by json_extract(c, '$.name') asc",
	}, {
		input:  `SELECT JSON_EXTRACT('{"a": 1, "b": 2, "c": {"d": 4}}', '$.a', @j)`,
		output: `select json_extract('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '$.a', @j) from dual`,
	}, {
		input:  "SELECT JSON_EXTRACT(@k, TRIM('abc'))",
		output: `select json_extract(@k, trim('abc')) from dual`,
	}, {
		input:  `SELECT JSON_KEYS('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '$.a')`,
		output: `select json_keys('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '$.a') from dual`,
	}, {
		input:  `SELECT JSON_KEYS('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}')`,
		output: `select json_keys('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}') from dual`,
	}, {
		input:  `SELECT JSON_OVERLAPS('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '$.a')`,
		output: `select json_overlaps('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '$.a') from dual`,
	}, {
		input:  "SELECT JSON_OVERLAPS(@j, @k)",
		output: "select json_overlaps(@j, @k) from dual",
	}, {
		input:  "SELECT JSON_OVERLAPS(@j, BIN(1))",
		output: "select json_overlaps(@j, BIN(1)) from dual",
	}, {
		input:  "SELECT JSON_SEARCH(@j, 'one', 'abc')",
		output: "select json_search(@j, 'one', 'abc') from dual",
	}, {
		input:  `SELECT JSON_SEARCH('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', @j, BIN(2))`,
		output: `select json_search('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', @j, BIN(2)) from dual`,
	}, {
		input:  `SELECT JSON_SEARCH('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', 'all', '10', NULL)`,
		output: `select json_search('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', 'all', '10', null) from dual`,
	}, {
		input:  "SELECT JSON_SEARCH(@j, 'all', '%b%', '', '$[3]')",
		output: "select json_search(@j, 'all', '%b%', '', '$[3]') from dual",
	}, {
		input:  "SELECT JSON_SEARCH(@j, 'all', '%b%', 'a', '$[3]')",
		output: "select json_search(@j, 'all', '%b%', 'a', '$[3]') from dual",
	}, {
		input:  `SELECT JSON_VALUE('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '$.a')`,
		output: `select json_value('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}', '$.a') from dual`,
	}, {
		input:  `SELECT JSON_VALUE(@j, @k)`,
		output: `select json_value(@j, @k) from dual`,
	}, {
		input:  `select json_value(@j, @k RETURNING FLOAT) from dual`,
		output: `select json_value(@j, @k returning FLOAT) from dual`,
	}, {
		input:  `select json_value(@j, @k RETURNING DECIMAL(4,2)) from dual`,
		output: `select json_value(@j, @k returning DECIMAL(4, 2)) from dual`,
	}, {
		input:  `SELECT JSON_VALUE('{"fname": "Joe", "lname": "Palmer"}', '$.fname' returning char(49) Charset utf8mb4 error on error)`,
		output: `select json_value('{\"fname\": \"Joe\", \"lname\": \"Palmer\"}', '$.fname' returning char(49) character set utf8mb4 error on error) from dual`,
	}, {
		input:  `SELECT JSON_VALUE('{"item": "shoes", "price": "49.95"}', '$.price' NULL ON EMPTY) `,
		output: `select json_value('{\"item\": \"shoes\", \"price\": \"49.95\"}', '$.price' null on empty) from dual`,
	}, {
		input:  `SELECT JSON_VALUE('{"item": "shoes", "price": "49.95"}', '$.price' NULL ON ERROR) `,
		output: `select json_value('{\"item\": \"shoes\", \"price\": \"49.95\"}', '$.price' null on error) from dual`,
	}, {
		input:  `SELECT JSON_VALUE('{"item": "shoes", "price": "49.95"}', '$.price' NULL ON EMPTY ERROR ON ERROR) `,
		output: `select json_value('{\"item\": \"shoes\", \"price\": \"49.95\"}', '$.price' null on empty error on error) from dual`,
	}, {
		input:  `select json_value(@j, @k RETURNING FLOAT NULL ON EMPTY ERROR ON ERROR) from dual`,
		output: `select json_value(@j, @k returning FLOAT null on empty error on error) from dual`,
	}, {
		input:  `SELECT 17 MEMBER OF ('[23, "abc", 17, "ab", 10]')`,
		output: `select 17 member of ('[23, \"abc\", 17, \"ab\", 10]') from dual`,
	}, {
		input:  "SELECT @j MEMBER OF (@k)",
		output: "select @j member of (@k) from dual",
	}, {
		input:  `SELECT 17 MEMBER OF('[23, "abc", "17", "ab", 10]'), "17" MEMBER OF('[23, "abc", 17, "ab", 10]')`,
		output: `select 17 member of ('[23, \"abc\", \"17\", \"ab\", 10]'), '17' member of ('[23, \"abc\", 17, \"ab\", 10]') from dual`,
	}, {
		input:  `SELECT JSON_DEPTH('{}'), JSON_DEPTH('[]'), JSON_DEPTH('true')`,
		output: `select json_depth('{}'), json_depth('[]'), json_depth('true') from dual`,
	}, {
		input:  `SELECT JSON_LENGTH('{"a": 1, "b": {"c": 30}}')`,
		output: `select json_length('{\"a\": 1, \"b\": {\"c\": 30}}') from dual`,
	}, {
		input:  `SELECT JSON_LENGTH('{"a": 1, "b": {"c": 30}}', '$.b');`,
		output: `select json_length('{\"a\": 1, \"b\": {\"c\": 30}}', '$.b') from dual`,
	}, {
		input:  `SELECT JSON_LENGTH('{\"a\": 1, \"b\": {\"c\": 30}}', @j);`,
		output: `select json_length('{\"a\": 1, \"b\": {\"c\": 30}}', @j) from dual`,
	}, {
		input:  `SELECT jcol, JSON_LENGTH(jcol)`,
		output: `select jcol, json_length(jcol) from dual`,
	}, {
		input:  "SELECT JSON_TYPE(@j)",
		output: "select json_type(@j) from dual",
	}, {
		input:  `SELECT JSON_TYPE(JSON_EXTRACT(@j, '$.a[0]'))`,
		output: `select json_type(json_extract(@j, '$.a[0]')) from dual`,
	}, {
		input:  `SELECT JSON_VALID('{\"a\": 1}')`,
		output: `select json_valid('{\"a\": 1}') from dual`,
	}, {
		input:  "SELECT JSON_VALID(@j)",
		output: "select json_valid(@j) from dual",
	}, {
		input:  `SELECT JSON_ARRAY_APPEND('{ "a": 1, "b": [2, 3]}','$[1]', 'x')`,
		output: `select json_array_append('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x') from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_APPEND('{ "a": 1, "b": [2, 3]}','$[1]', 'x', '$[2]', 1)`,
		output: `select json_array_append('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x', '$[2]', 1) from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_APPEND('{ "a": 1, "b": [2, 3]}','$[1]', 'x', @i, @j)`,
		output: `select json_array_append('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x', @i, @j) from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_APPEND('{ "a": 1, "b": [2, 3]}', @j, 1)`,
		output: `select json_array_append('{ \"a\": 1, \"b\": [2, 3]}', @j, 1) from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_APPEND('{ "a": 1, "b": [2, 3]}', '$[1]', @j)`,
		output: `select json_array_append('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', @j) from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_APPEND('{ "a": 1, "b": [2, 3]}', @j, @k)`,
		output: `select json_array_append('{ \"a\": 1, \"b\": [2, 3]}', @j, @k) from dual`,
	}, {
		input:  "SELECT JSON_ARRAY_APPEND(@i,@j,@k)",
		output: `select json_array_append(@i, @j, @k) from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_INSERT('{ "a": 1, "b": [2, 3]}','$[1]', 'x')`,
		output: `select json_array_insert('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x') from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_INSERT('{ "a": 1, "b": [2, 3]}','$[1]', 'x', '$[2]', 1)`,
		output: `select json_array_insert('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x', '$[2]', 1) from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_INSERT('{ "a": 1, "b": [2, 3]}','$[1]', 'x', @i, @j)`,
		output: `select json_array_insert('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x', @i, @j) from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_INSERT('{ "a": 1, "b": [2, 3]}', @j, 1)`,
		output: `select json_array_insert('{ \"a\": 1, \"b\": [2, 3]}', @j, 1) from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_INSERT('{ "a": 1, "b": [2, 3]}', '$[1]', @j)`,
		output: `select json_array_insert('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', @j) from dual`,
	}, {
		input:  `SELECT JSON_ARRAY_INSERT('{ "a": 1, "b": [2, 3]}', @j, @k)`,
		output: `select json_array_insert('{ \"a\": 1, \"b\": [2, 3]}', @j, @k) from dual`,
	}, {
		input:  "SELECT JSON_ARRAY_INSERT(@i,@j,@k)",
		output: "select json_array_insert(@i, @j, @k) from dual",
	}, {
		input:  "SELECT JSON_ARRAY_INSERT(@j, '$[0]', 'x', '$[2][1]', 'y')",
		output: "select json_array_insert(@j, '$[0]', 'x', '$[2][1]', 'y') from dual",
	}, {
		input:  `SELECT JSON_INSERT('{ "a": 1, "b": [2, 3]}','$[1]', 'x')`,
		output: `select json_insert('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x') from dual`,
	}, {
		input:  `SELECT JSON_INSERT('{ "a": 1, "b": [2, 3]}','$[1]', 'x', '$[2]', 1)`,
		output: `select json_insert('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x', '$[2]', 1) from dual`,
	}, {
		input:  `SELECT JSON_INSERT('{ "a": 1, "b": [2, 3]}','$[1]', 'x', @i, @j)`,
		output: `select json_insert('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x', @i, @j) from dual`,
	}, {
		input:  `SELECT JSON_INSERT('{ "a": 1, "b": [2, 3]}', @j, 1)`,
		output: `select json_insert('{ \"a\": 1, \"b\": [2, 3]}', @j, 1) from dual`,
	}, {
		input:  `SELECT JSON_INSERT('{ "a": 1, "b": [2, 3]}', '$[1]', @j)`,
		output: `select json_insert('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', @j) from dual`,
	}, {
		input:  `SELECT JSON_INSERT('{ "a": 1, "b": [2, 3]}', @j, @k)`,
		output: `select json_insert('{ \"a\": 1, \"b\": [2, 3]}', @j, @k) from dual`,
	}, {
		input:  "SELECT JSON_INSERT(@i,@j,@k)",
		output: "select json_insert(@i, @j, @k) from dual",
	}, {
		input:  "SELECT JSON_INSERT(@j, '$.a', 10, '$.c', '[true, false]')",
		output: "select json_insert(@j, '$.a', 10, '$.c', '[true, false]') from dual",
	}, {
		input:  `SELECT JSON_REPLACE('{ "a": 1, "b": [2, 3]}','$[1]', 'x')`,
		output: `select json_replace('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x') from dual`,
	}, {
		input:  `SELECT JSON_REPLACE('{ "a": 1, "b": [2, 3]}','$[1]', 'x', '$[2]', 1)`,
		output: `select json_replace('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x', '$[2]', 1) from dual`,
	}, {
		input:  `SELECT JSON_REPLACE('{ "a": 1, "b": [2, 3]}','$[1]', 'x', @i, @j)`,
		output: `select json_replace('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x', @i, @j) from dual`,
	}, {
		input:  `SELECT JSON_REPLACE('{ "a": 1, "b": [2, 3]}', @j, 1)`,
		output: `select json_replace('{ \"a\": 1, \"b\": [2, 3]}', @j, 1) from dual`,
	}, {
		input:  `SELECT JSON_REPLACE('{ "a": 1, "b": [2, 3]}', '$[1]', @j)`,
		output: `select json_replace('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', @j) from dual`,
	}, {
		input:  `SELECT JSON_REPLACE('{ "a": 1, "b": [2, 3]}', @j, @k)`,
		output: `select json_replace('{ \"a\": 1, \"b\": [2, 3]}', @j, @k) from dual`,
	}, {
		input:  "SELECT JSON_REPLACE(@i,@j,@k)",
		output: "select json_replace(@i, @j, @k) from dual",
	}, {
		input:  "SELECT JSON_REPLACE(@j, '$.a', 10, '$.c', '[true, false]')",
		output: "select json_replace(@j, '$.a', 10, '$.c', '[true, false]') from dual",
	}, {
		input:  `SELECT JSON_SET('{ "a": 1, "b": [2, 3]}','$[1]', 'x')`,
		output: `select json_set('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x') from dual`,
	}, {
		input:  `SELECT JSON_SET('{ "a": 1, "b": [2, 3]}','$[1]', 'x', '$[2]', 1)`,
		output: `select json_set('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x', '$[2]', 1) from dual`,
	}, {
		input:  `SELECT JSON_SET('{ "a": 1, "b": [2, 3]}','$[1]', 'x', @i, @j)`,
		output: `select json_set('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', 'x', @i, @j) from dual`,
	}, {
		input:  `SELECT JSON_SET('{ "a": 1, "b": [2, 3]}', @j, 1)`,
		output: `select json_set('{ \"a\": 1, \"b\": [2, 3]}', @j, 1) from dual`,
	}, {
		input:  `SELECT JSON_SET('{ "a": 1, "b": [2, 3]}', '$[1]', @j)`,
		output: `select json_set('{ \"a\": 1, \"b\": [2, 3]}', '$[1]', @j) from dual`,
	}, {
		input:  `SELECT JSON_SET('{ "a": 1, "b": [2, 3]}', @j, @k)`,
		output: `select json_set('{ \"a\": 1, \"b\": [2, 3]}', @j, @k) from dual`,
	}, {
		input:  "SELECT JSON_SET(@i,@j,@k)",
		output: "select json_set(@i, @j, @k) from dual",
	}, {
		input:  "SELECT JSON_SET(@j, '$.a', 10, '$.c', '[true, false]')",
		output: "select json_set(@j, '$.a', 10, '$.c', '[true, false]') from dual",
	}, {
		input:  "SELECT JSON_MERGE('[1, 2]', '[true, false]')",
		output: "select json_merge('[1, 2]', '[true, false]') from dual",
	}, {
		input:  "SELECT JSON_MERGE('[1, 2]', '[true, false]', 'hello')",
		output: "select json_merge('[1, 2]', '[true, false]', 'hello') from dual",
	}, {
		input:  "SELECT JSON_MERGE('[1, 2]', @i)",
		output: "select json_merge('[1, 2]', @i) from dual",
	}, {
		input:  "SELECT JSON_MERGE(@i, @j)",
		output: "select json_merge(@i, @j) from dual",
	}, {
		input:  "SELECT JSON_MERGE(@i, @j, @k)",
		output: "select json_merge(@i, @j, @k) from dual",
	}, {
		input:  "SELECT JSON_MERGE(@i, '[true, false]')",
		output: "select json_merge(@i, '[true, false]') from dual",
	}, {
		input:  `SELECT JSON_MERGE_PATCH('{"name": "x"}', '{"id": 47}')`,
		output: `select json_merge_patch('{\"name\": \"x\"}', '{\"id\": 47}') from dual`,
	}, {
		input:  `SELECT JSON_MERGE_PATCH('{ "a": 1, "b":2 }','{ "a": 3, "c":4 }','{ "a": 5, "d":6 }');`,
		output: `select json_merge_patch('{ \"a\": 1, \"b\":2 }', '{ \"a\": 3, \"c\":4 }', '{ \"a\": 5, \"d\":6 }') from dual`,
	}, {
		input:  "SELECT JSON_MERGE_PATCH('[1, 2]', '[true, false]', 'hello')",
		output: "select json_merge_patch('[1, 2]', '[true, false]', 'hello') from dual",
	}, {
		input:  "SELECT JSON_MERGE_PATCH('[1, 2]', @i)",
		output: "select json_merge_patch('[1, 2]', @i) from dual",
	}, {
		input:  "SELECT JSON_MERGE_PATCH(@i, @j)",
		output: "select json_merge_patch(@i, @j) from dual",
	}, {
		input:  "SELECT JSON_MERGE_PATCH(@i, '[true, false]')",
		output: "select json_merge_patch(@i, '[true, false]') from dual",
	}, {
		input:  `SELECT JSON_MERGE_PRESERVE('{"name": "x"}', '{"id": 47}')`,
		output: `select json_merge_preserve('{\"name\": \"x\"}', '{\"id\": 47}') from dual`,
	}, {
		input:  `SELECT JSON_MERGE_PRESERVE('{ "a": 1, "b":2 }','{ "a": 3, "c":4 }','{ "a": 5, "d":6 }');`,
		output: `select json_merge_preserve('{ \"a\": 1, \"b\":2 }', '{ \"a\": 3, \"c\":4 }', '{ \"a\": 5, \"d\":6 }') from dual`,
	}, {
		input:  "SELECT JSON_MERGE_PRESERVE('[1, 2]', '[true, false]', 'hello')",
		output: "select json_merge_preserve('[1, 2]', '[true, false]', 'hello') from dual",
	}, {
		input:  "SELECT JSON_MERGE_PRESERVE('[1, 2]', @i)",
		output: "select json_merge_preserve('[1, 2]', @i) from dual",
	}, {
		input:  "SELECT JSON_MERGE_PRESERVE(@i, @j)",
		output: "select json_merge_preserve(@i, @j) from dual",
	}, {
		input:  "SELECT JSON_MERGE_PRESERVE(@i, '[true, false]')",
		output: "select json_merge_preserve(@i, '[true, false]') from dual",
	}, {
		input:  "SELECT JSON_REMOVE(@i, '$[1]')",
		output: "select json_remove(@i, '$[1]') from dual",
	}, {
		input:  `SELECT JSON_REMOVE('["a", ["b", "c"], "d"]', '$[1]')`,
		output: `select json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$[1]') from dual`,
	}, {
		input:  `SELECT JSON_REMOVE('["a", ["b", "c"], "d"]', @i)`,
		output: `select json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', @i) from dual`,
	}, {
		input:  `SELECT JSON_REMOVE('["a", ["b", "c"], "d"]', @i, @j, '$[0]', '$[1]','$[2]')`,
		output: `select json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', @i, @j, '$[0]', '$[1]', '$[2]') from dual`,
	}, {
		input:  "SELECT JSON_UNQUOTE('abc')",
		output: "select json_unquote('abc') from dual",
	}, {
		input:  `SELECT JSON_UNQUOTE('\"\\\\t\\\\u0032\"')`,
		output: `select json_unquote('\"\\\\t\\\\u0032\"') from dual`,
	}, {
		input:  "SELECT JSON_UNQUOTE(@j)",
		output: "select json_unquote(@j) from dual",
	}, {
		input:  "CREATE TABLE ts (id INT, purchased DATE) PARTITION BY RANGE( YEAR(purchased) ) SUBPARTITION BY HASH( TO_DAYS(purchased) ) ( PARTITION p0 VALUES LESS THAN (1990) (SUBPARTITION s0,SUBPARTITION s1),PARTITION p1 VALUES LESS THAN (2000),PARTITION p2 VALUES LESS THAN MAXVALUE (SUBPARTITION s2,SUBPARTITION s3));",
		output: "create table ts (\n\tid INT,\n\tpurchased DATE\n)\npartition by range (YEAR(purchased)) subpartition by hash (TO_DAYS(purchased))\n(partition p0 values less than (1990) (subpartition s0, subpartition s1),\n partition p1 values less than (2000),\n partition p2 values less than maxvalue (subpartition s2, subpartition `s3`))",
	}, {
		input:  "SELECT REGEXP_INSTR('dog cat dog', 'dog')",
		output: "select regexp_instr('dog cat dog', 'dog') from dual",
	}, {
		input:  "SELECT REGEXP_INSTR('aa aaa aaaa aaaa aaaa aaaa', 'a{4}',1)",
		output: "select regexp_instr('aa aaa aaaa aaaa aaaa aaaa', 'a{4}', 1) from dual",
	}, {
		input:  "SELECT REGEXP_INSTR('aa aaa aaaa aaaa aaaa aaaa', 'a{4}',1,TRIM('2'))",
		output: "select regexp_instr('aa aaa aaaa aaaa aaaa aaaa', 'a{4}', 1, trim('2')) from dual",
	}, {
		input:  "SELECT REGEXP_INSTR('aa aaa aaaa aaaa aaaa aaaa', 'a{4}',1,TRIM('2'),0)",
		output: "select regexp_instr('aa aaa aaaa aaaa aaaa aaaa', 'a{4}', 1, trim('2'), 0) from dual",
	}, {
		input:  "SELECT REGEXP_INSTR('aa aaa aaaa aaaa aaaa aaaa', 'a{4}',1,TRIM('2'),0, 'c')",
		output: "select regexp_instr('aa aaa aaaa aaaa aaaa aaaa', 'a{4}', 1, trim('2'), 0, 'c') from dual",
	}, {
		input:  "SELECT REGEXP_LIKE('dog cat dog', 'dog')",
		output: "select regexp_like('dog cat dog', 'dog') from dual",
	}, {
		input:  "SELECT NOT REGEXP_LIKE('dog cat dog', 'dog')",
		output: "select not regexp_like('dog cat dog', 'dog') from dual",
	}, {
		input:  "SELECT REGEXP_LIKE('aa aaa aaaa aaaa aaaa aaaa', 'a{4}',1)",
		output: "select regexp_like('aa aaa aaaa aaaa aaaa aaaa', 'a{4}', 1) from dual",
	}, {
		input:  "SELECT REGEXP_REPLACE('a b c', 'b', 'X');",
		output: "select regexp_replace('a b c', 'b', 'X') from dual",
	}, {
		input:  "SELECT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1);",
		output: "select regexp_replace('abc def ghi', '[a-z]+', 'X', 1) from dual",
	}, {
		input:  "SELECT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3);",
		output: "select regexp_replace('abc def ghi', '[a-z]+', 'X', 1, 3) from dual",
	}, {
		input:  "SELECT REGEXP_REPLACE('abc def ghi', '[a-z]+', 'X', 1, 3, 'c');",
		output: "select regexp_replace('abc def ghi', '[a-z]+', 'X', 1, 3, 'c') from dual",
	}, {
		input:  "SELECT REGEXP_SUBSTR('abc def ghi', '[a-z]+');",
		output: "select regexp_substr('abc def ghi', '[a-z]+') from dual",
	}, {
		input:  "SELECT REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1);",
		output: "select regexp_substr('abc def ghi', '[a-z]+', 1) from dual",
	}, {
		input:  "SELECT REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1, 3);",
		output: "select regexp_substr('abc def ghi', '[a-z]+', 1, 3) from dual",
	}, {
		input:  "SELECT REGEXP_SUBSTR('abc def ghi', '[a-z]+', 1, 3, TRIM(' n '));",
		output: "select regexp_substr('abc def ghi', '[a-z]+', 1, 3, trim(' n ')) from dual",
	}, {
		input:  "SELECT 'Michael!' RLIKE '.*';",
		output: "select 'Michael!' regexp '.*' from dual",
	}, {
		input:  "SELECT TRIM('Michael!') RLIKE @j",
		output: "select trim('Michael!') regexp @j from dual",
	}, {
		input:  "SELECT INSERT('Quadratic', 3, 4, 'What')",
		output: "select insert('Quadratic', 3, 4, 'What') from dual",
	}, {
		input:  "SELECT INTERVAL(1, 3, 4)",
		output: "select interval(1, 3, 4) from dual",
	}, {
		input:  "SELECT POSITION('bar' IN 'foobarbar')",
		output: "select locate('bar', 'foobarbar') from dual",
	}, {
		input:  "SELECT CHAR(77,121,83,81,'76' USING utf8mb4)",
		output: "select char(77, 121, 83, 81, '76' using utf8mb4) from dual",
	}, {
		input:  "SELECT val, CUME_DIST() OVER w, ROW_NUMBER() OVER w, DENSE_RANK() OVER w, PERCENT_RANK() OVER w, RANK() OVER w AS 'cd' FROM numbers",
		output: "select val, cume_dist() over w, row_number() over w, dense_rank() over w, percent_rank() over w, rank() over w as cd from numbers",
	}, {
		input:  "SELECT year, country, product, profit, CUME_DIST() OVER() AS total_profit FROM sales",
		output: "select `year`, country, product, profit, cume_dist() over () as total_profit from sales",
	}, {
		input:  "SELECT val, CUME_DIST() OVER (ORDER BY val) AS 'cd' FROM numbers",
		output: "select val, cume_dist() over ( order by val asc) as cd from numbers",
	}, {
		input:  "SELECT val, CUME_DIST() OVER (PARTITION BY z ORDER BY val, subject DESC ROWS CURRENT ROW) AS 'cd' FROM numbers",
		output: "select val, cume_dist() over ( partition by z order by val asc, subject desc rows current row) as cd from numbers",
	}, {
		input:  "SELECT val, CUME_DIST() OVER (val PARTITION BY z, subject ORDER BY val, subject DESC ROWS CURRENT ROW) AS 'cd' FROM numbers",
		output: "select val, cume_dist() over ( val partition by z, subject order by val asc, subject desc rows current row) as cd from numbers",
	}, {
		input:  "SELECT val, FIRST_VALUE(val) OVER w FROM numbers",
		output: "select val, first_value(val) over w from numbers",
	}, {
		input:  "SELECT val, LAST_VALUE(val) IGNORE NULLS OVER w FROM numbers",
		output: "select val, last_value(val) ignore nulls over w from numbers",
	}, {
		input:  "SELECT NTILE(1) OVER w, NTILE(0) OVER w  FROM numbers",
		output: "select ntile(1) over w, ntile(0) over w from numbers",
	}, {
		input:  "SELECT NTILE(NULL) OVER w FROM numbers",
		output: "select ntile(null) over w from numbers",
	}, {
		input:  "SELECT NTILE(@val) OVER w FROM numbers",
		output: "select ntile(@val) over w from numbers",
	}, {
		input:  "SELECT NTH_VALUE(@z,1) OVER w, NTH_VALUE('val',0) OVER w FROM numbers",
		output: "select nth_value(@z, 1) over w, nth_value('val', 0) over w from numbers",
	}, {
		input:  "SELECT NTH_VALUE(val,NULL) FROM FIRST OVER w FROM numbers",
		output: "select nth_value(val, null) respect nulls over w from numbers",
	}, {
		input:  "SELECT NTH_VALUE(val,NULL) RESPECT NULLS OVER w FROM numbers",
		output: "select nth_value(val, null) respect nulls over w from numbers",
	}, {
		input:  "SELECT LAG(val) OVER w, LEAD(TRIM('abc')) OVER w FROM numbers",
		output: "select lag(val) over w, lead(trim('abc')) over w from numbers",
	}, {
		input:  "SELECT LAG(val, 10) OVER w, LEAD('val', null) OVER w, LEAD(val, 1, ASCII(1)) OVER w FROM numbers",
		output: "select lag(val, 10) over w, lead('val', null) over w, lead(val, 1, ASCII(1)) over w from numbers",
	}, {
		input:  "SELECT val, ROW_NUMBER() OVER (ORDER BY val) AS 'row_number' FROM numbers WINDOW w AS (ORDER BY val);",
		output: "select val, row_number() over ( order by val asc) as `row_number` from numbers window w AS ( order by val asc)",
	}, {
		input:  "SELECT time, subject, val, FIRST_VALUE(val)  OVER w AS 'first', LAST_VALUE(val) OVER w AS 'last', NTH_VALUE(val, 2) OVER w AS 'second', NTH_VALUE(val, 4) OVER w AS 'fourth' FROM observations WINDOW w AS (PARTITION BY subject ORDER BY time ROWS UNBOUNDED PRECEDING);",
		output: "select `time`, subject, val, first_value(val) over w as `first`, last_value(val) over w as `last`, nth_value(val, 2) over w as `second`, nth_value(val, 4) over w as fourth from observations window w AS ( partition by subject order by `time` asc rows unbounded preceding)",
	}, {
		input:  "SELECT time, subject, val, FIRST_VALUE(val)  OVER w AS 'first', LAST_VALUE(val) OVER w AS 'last', NTH_VALUE(val, 2) OVER w AS 'second', NTH_VALUE(val, 4) OVER w AS 'fourth' FROM observations WINDOW w AS (PARTITION BY subject ORDER BY time RANGE 10 PRECEDING);",
		output: "select `time`, subject, val, first_value(val) over w as `first`, last_value(val) over w as `last`, nth_value(val, 2) over w as `second`, nth_value(val, 4) over w as fourth from observations window w AS ( partition by subject order by `time` asc range 10 preceding)",
	}, {
		input:  "SELECT time, subject, val, FIRST_VALUE(val)  OVER w AS 'first', LAST_VALUE(val) OVER w AS 'last', NTH_VALUE(val, 2) OVER w AS 'second', NTH_VALUE(val, 4) OVER w AS 'fourth' FROM observations WINDOW w AS (PARTITION BY subject ORDER BY time ROWS INTERVAL 5 DAY PRECEDING);",
		output: "select `time`, subject, val, first_value(val) over w as `first`, last_value(val) over w as `last`, nth_value(val, 2) over w as `second`, nth_value(val, 4) over w as fourth from observations window w AS ( partition by subject order by `time` asc rows interval 5 day preceding)",
	}, {
		input:  "SELECT time, subject, val, FIRST_VALUE(val)  OVER w AS 'first', LAST_VALUE(val) OVER w AS 'last', NTH_VALUE(val, 2) OVER w AS 'second', NTH_VALUE(val, 4) OVER w AS 'fourth' FROM observations WINDOW w AS (PARTITION BY subject ORDER BY time RANGE 5 FOLLOWING);",
		output: "select `time`, subject, val, first_value(val) over w as `first`, last_value(val) over w as `last`, nth_value(val, 2) over w as `second`, nth_value(val, 4) over w as fourth from observations window w AS ( partition by subject order by `time` asc range 5 following)",
	}, {
		input:  "SELECT time, subject, val, FIRST_VALUE(val)  OVER w AS 'first', LAST_VALUE(val) OVER w AS 'last', NTH_VALUE(val, 2) OVER w AS 'second', NTH_VALUE(val, 4) OVER w AS 'fourth' FROM observations WINDOW w AS (PARTITION BY subject ORDER BY time ROWS INTERVAL '2:30' MINUTE_SECOND FOLLOWING);",
		output: "select `time`, subject, val, first_value(val) over w as `first`, last_value(val) over w as `last`, nth_value(val, 2) over w as `second`, nth_value(val, 4) over w as fourth from observations window w AS ( partition by subject order by `time` asc rows interval '2:30' minute_second following)",
	}, {
		input:  "SELECT time, subject, val, FIRST_VALUE(val)  OVER w AS 'first', LAST_VALUE(val) OVER w AS 'last', NTH_VALUE(val, 2) OVER w AS 'second', NTH_VALUE(val, 4) OVER w AS 'fourth' FROM observations WINDOW w AS (PARTITION BY subject ORDER BY time ASC RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING);",
		output: "select `time`, subject, val, first_value(val) over w as `first`, last_value(val) over w as `last`, nth_value(val, 2) over w as `second`, nth_value(val, 4) over w as fourth from observations window w AS ( partition by subject order by `time` asc range between 10 preceding and 10 following)",
	}, {
		input:  "SELECT ExtractValue('<a><b/></a>', '/a/b')",
		output: "select extractvalue('<a><b/></a>', '/a/b') from dual",
	}, {
		input:  "SELECT @i, ExtractValue(@xml, '//b[$@i]')",
		output: "select @i, extractvalue(@xml, '//b[$@i]') from dual",
	}, {
		input:  "SELECT ExtractValue(TRIM('<a><c/></a>'), 'count(/a/b)')",
		output: "select extractvalue(trim('<a><c/></a>'), 'count(/a/b)') from dual",
	}, {
		input:  "SELECT UpdateXML(@xml, '//b:c', '<g:h>555</g:h>')",
		output: "select updatexml(@xml, '//b:c', '<g:h>555</g:h>') from dual",
	}, {
		input:  "SELECT UpdateXML('<a><d></d><b>ccc</b><d></d></a>', '/a/d', '<e>fff</e>') AS val5",
		output: "select updatexml('<a><d></d><b>ccc</b><d></d></a>', '/a/d', '<e>fff</e>') as val5 from dual",
	}, {
		input: "select get_lock('a', 10), is_free_lock('b'), is_used_lock('c'), release_all_locks(), release_lock('d') from dual",
	}, {
		input: "select /* function with distinct */ count(a) from t",
	}, {
		input:  "select /* function with distinct */ count(a) 'total col' from t",
		output: "select /* function with distinct */ count(a) as `total col` from t",
	}, {
		input: "select /* function with distinct */ count(distinct a) from t",
	}, {
		input:  "select /* function with distinct */ count(distinct(a)) from t",
		output: "select /* function with distinct */ count(distinct a) from t",
	}, {
		input: "select /* function with distinct */ count(*) from t",
	}, {
		input: "select avg(a) from products",
	}, {
		input:  "select avg(distinct(a)) from products",
		output: "select avg(distinct a) from products",
	}, {
		input:  "select avg(a) 'Avg Price' from products",
		output: "select avg(a) as `Avg Price` from products",
	}, {
		input: "select format(avg(distinct a), 2) from products",
	}, {
		input: "select max(a) from products",
	}, {
		input: "select min(a) from products",
	}, {
		input: "select sum(a) from products",
	}, {
		input:  "select sum(distinct(a)) from products",
		output: "select sum(distinct a) from products",
	}, {
		input: "select sum(distinct a) from products",
	}, {
		input:  "select sum(a) 'sum Price' from products",
		output: "select sum(a) as `sum Price` from products",
	}, {
		input: "select sum(a * b) from products",
	}, {
		input: "select bit_and(a) from products",
	}, {
		input: "select bit_or(a) from products",
	}, {
		input: "select bit_xor(a) from products",
	}, {
		input: "select std(a) from products",
	}, {
		input: "select stddev(a) from products",
	}, {
		input: "select stddev_pop(a) from products",
	}, {
		input: "select stddev_samp(a) from products",
	}, {
		input: "select var_pop(a) from products",
	}, {
		input: "select var_samp(a) from products",
	}, {
		input: "select variance(a) from products",
	}, {
		input:  "SELECT FORMAT_BYTES(512), FORMAT_BYTES(18446644073709551615), FORMAT_BYTES(@j), FORMAT_BYTES('asd'), FORMAT_BYTES(TRIM('str'))",
		output: "select format_bytes(512), format_bytes(18446644073709551615), format_bytes(@j), format_bytes('asd'), format_bytes(trim('str')) from dual",
	}, {
		input:  "SELECT FORMAT_PICO_TIME(512), FORMAT_PICO_TIME(18446644073709551615), FORMAT_PICO_TIME(@j), FORMAT_PICO_TIME('asd'), FORMAT_PICO_TIME(TRIM('str'))",
		output: "select format_pico_time(512), format_pico_time(18446644073709551615), format_pico_time(@j), format_pico_time('asd'), format_pico_time(trim('str')) from dual",
	}, {
		input:  "SELECT PS_CURRENT_THREAD_ID()",
		output: "select ps_current_thread_id() from dual",
	}, {
		input:  "SELECT PS_THREAD_ID(512), PS_THREAD_ID(18446644073709551615), PS_THREAD_ID(@j), PS_THREAD_ID('asd'), PS_THREAD_ID(TRIM('str'))",
		output: "select ps_thread_id(512), ps_thread_id(18446644073709551615), ps_thread_id(@j), ps_thread_id('asd'), ps_thread_id(trim('str')) from dual",
	}, {
		input:  "SELECT GTID_SUBSET('3E11FA47-71CA-11E1-9E33-C80AA9429562:23','3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57')",
		output: "select gtid_subset('3E11FA47-71CA-11E1-9E33-C80AA9429562:23', '3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57') from dual",
	}, {
		input:  "SELECT GTID_SUBSET(@j,TRIM('3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57'))",
		output: "select gtid_subset(@j, trim('3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57')) from dual",
	}, {
		input:  "SELECT GTID_SUBTRACT('3E11FA47-71CA-11E1-9E33-C80AA9429562:23','3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57')",
		output: "select gtid_subtract('3E11FA47-71CA-11E1-9E33-C80AA9429562:23', '3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57') from dual",
	}, {
		input:  "SELECT GTID_SUBTRACT(@j,TRIM('3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57'))",
		output: "select gtid_subtract(@j, trim('3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57')) from dual",
	}, {
		input:  "SELECT WAIT_FOR_EXECUTED_GTID_SET('3E11FA47-71CA-11E1-9E33-C80AA9429562:23')",
		output: "select wait_for_executed_gtid_set('3E11FA47-71CA-11E1-9E33-C80AA9429562:23') from dual",
	}, {
		input:  "SELECT WAIT_FOR_EXECUTED_GTID_SET(TRIM('3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57'), @j)",
		output: "select wait_for_executed_gtid_set(trim('3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57'), @j) from dual",
	}, {
		input:  "SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS('3E11FA47-71CA-11E1-9E33-C80AA9429562:23')",
		output: "select wait_until_sql_thread_after_gtids('3E11FA47-71CA-11E1-9E33-C80AA9429562:23') from dual",
	}, {
		input:  "SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS(TRIM('3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57'), @j)",
		output: "select wait_until_sql_thread_after_gtids(trim('3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57'), @j) from dual",
	}, {
		input:  "SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS(TRIM('3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57'), 10, @i)",
		output: "select wait_until_sql_thread_after_gtids(trim('3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57'), 10, @i) from dual",
	}, {
		// Offset as part of expressions
		input: "select a, b from c where :1 + :2 = :302 and sum(:34) < :24",
	}, {
		input:  "select * from (((select 1))) as tbl",
		output: "select * from (select 1 from dual) as tbl",
	}, {
		input:  `select * from t1 where col1 like 'ks\%' and col2 = 'ks\%' and col1 like 'ks%' and col2 = 'ks%'`,
		output: `select * from t1 where col1 like 'ks\%' and col2 = 'ks\%' and col1 like 'ks%' and col2 = 'ks%'`,
	}, {
		input:  `select * from t1 where col1 like 'ks\_' and col2 = 'ks\_' and col1 like 'ks_' and col2 = 'ks_'`,
		output: `select * from t1 where col1 like 'ks\_' and col2 = 'ks\_' and col1 like 'ks_' and col2 = 'ks_'`,
	}}
)

func TestValid(t *testing.T) {
	for _, tcase := range validSQL {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.output == "" {
				tcase.output = tcase.input
			}
			tree, err := Parse(tcase.input)
			require.NoError(t, err, tcase.input)
			out := String(tree)
			assert.Equal(t, tcase.output, out)

			// Some statements currently only have 5.7 specifications.
			// For mysql 8.0 syntax, the query is not entirely parsed.
			// Add more structs as we go on adding full parsing support for DDL constructs for 5.7 syntax.
			switch x := tree.(type) {
			case DBDDLStatement:
				assert.Equal(t, !tcase.partialDDL, x.IsFullyParsed())
			case DDLStatement:
				assert.Equal(t, !tcase.partialDDL, x.IsFullyParsed())
			}
			// This test just exercises the tree walking functionality.
			// There's no way automated way to verify that a node calls
			// all its children. But we can examine code coverage and
			// ensure that all walkSubtree functions were called.
			_ = Walk(func(node SQLNode) (bool, error) {
				return true, nil
			}, tree)
		})
	}
}

// Ensure there is no corruption from using a pooled yyParserImpl in Parse.
func TestParallelValid(t *testing.T) {
	parallelism := 100
	numIters := 1000

	wg := sync.WaitGroup{}
	wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIters; j++ {
				tcase := validSQL[rand.Intn(len(validSQL))]
				if tcase.output == "" {
					tcase.output = tcase.input
				}
				tree, err := Parse(tcase.input)
				if err != nil {
					t.Errorf("Parse(%q) err: %v, want nil", tcase.input, err)
					continue
				}
				out := String(tree)
				if out != tcase.output {
					t.Errorf("Parse(%q) = %q, want: %q", tcase.input, out, tcase.output)
				}
			}
		}()
	}
	wg.Wait()
}

func TestInvalid(t *testing.T) {
	invalidSQL := []struct {
		input string
		err   string
	}{{
		input: "select a, b from (select * from tbl) sort by a",
		err:   "syntax error",
	}, {
		input: "/*!*/",
		err:   "Query was empty",
	}, {
		input: "select /* union with limit on lhs */ 1 from t limit 1 union select 1 from t",
		err:   "syntax error at position 60 near 'union'",
	}, {
		input: "(select * from t limit 100 into outfile s3 'out_file_name') union (select * from t2)",
		err:   "syntax error",
	}, {
		input: "select * from (select * from t into outfile s3 'inner_outfile') as t2 into outfile s3 'out_file_name'",
		err:   "syntax error at position 36 near 'into'",
	}, {
		input: "select a from x order by y union select a from c",
		err:   "syntax error",
	}, {
		input: "select `name`, numbers from (select * from users) as x()",
		err:   "syntax error at position 57",
	}, {
		input: "select next 2 values from seq union select next value from seq",
		err:   "syntax error at position 36 near 'union'",
	}, {
		input: "select next 2 values from user where id = 1",
		err:   "syntax error at position 37 near 'where'",
	}, {
		input: "select next 2 values from seq, seq",
		err:   "syntax error at position 31",
	}, {
		input: "select 1, next value from seq",
		err:   "syntax error",
	}, {
		input: "SELECT jcol, JSON_PRETTY(jcol, jcol) from jtable",
		err:   "syntax error at position 31",
	}, {
		input: "SELECT JSON_ARRAY(1,)",
		err:   "syntax error at position 22",
	}, {
		input: "SELECT JSON_OBJECT(1)",
		err:   "syntax error at position 22",
	}, {
		input: "SELECT JSON_OBJECT(1,2,)",
		err:   "syntax error at position 25",
	}, {
		input: "SELECT JSON_OBJECT(1,)",
		err:   "syntax error at position 23",
	}, {
		input: "SELECT JSON_QUOTE()",
		err:   "syntax error at position 20",
	}, {
		input: "select from t1, lateral (with qn as (select t1.a) select (select max(a) from qn)) as dt",
		err:   "syntax error at position 12 near 'from'",
	}, {
		input: `SELECT JSON_SCHEMA_VALID('{"type":"string","pattern":"("}')`,
		err:   `syntax error at position 60`,
	}, {
		input: `SELECT JSON_SCHEMA_VALIDATION_REPORT('{"type":"string","pattern":"("}')`,
		err:   `syntax error at position 72`,
	}, {
		input: "SELECT JSON_CONTAINS(@j, @j2, )",
		err:   "syntax error at position 32",
	}, {
		input: "SELECT JSON_CONTAINS_PATH(@j, @j2)",
		err:   "syntax error at position 35",
	}, {
		input: "SELECT JSON_EXTRACT(@k)",
		err:   "syntax error at position 24",
	}, {
		input: `SELECT JSON_KEYS('{\"a\": 1, \"b\": 2, \"c\": {\"d\": 4}}',)`,
		err:   `syntax error at position 61`,
	}, {
		input: "SELECT JSON_ARRAY_APPEND('{ \"a\": 1, \"b\": [2, 3]}')",
		err:   "syntax error at position 51",
	}, {
		input: "SELECT JSON_ARRAY_APPEND('{ \"a\": 1, \"b\": [2, 3]}','$[1]',)",
		err:   "syntax error at position 59",
	}, {
		input: "SELECT JSON_ARRAY_INSERT('{ \"a\": 1, \"b\": [2, 3]}')",
		err:   "syntax error at position 51",
	}, {
		input: "SELECT JSON_ARRAY_INSERT('{ \"a\": 1, \"b\": [2, 3]}','$[1]',)",
		err:   "syntax error at position 59",
	}, {
		input: "SELECT JSON_INSERT('{ \"a\": 1, \"b\": [2, 3]}')",
		err:   "syntax error at position 45",
	}, {
		input: "SELECT JSON_INSERT('{ \"a\": 1, \"b\": [2, 3]}','$[1]',)",
		err:   "syntax error at position 53",
	}, {
		input: "SELECT JSON_REPLACE('{ \"a\": 1, \"b\": [2, 3]}')",
		err:   "syntax error at position 46",
	}, {
		input: "SELECT JSON_REPLACE('{ \"a\": 1, \"b\": [2, 3]}','$[1]',)",
		err:   "syntax error at position 54",
	}, {
		input: "SELECT JSON_SET('{ \"a\": 1, \"b\": [2, 3]}')",
		err:   "syntax error at position 42",
	}, {
		input: "SELECT JSON_SET('{ \"a\": 1, \"b\": [2, 3]}','$[1]',)",
		err:   "syntax error at position 50",
	}, {
		input: "SELECT JSON_MERGE('[1, 2]')",
		err:   "syntax error at position 28",
	}, {
		input: "SELECT JSON_MERGE_PATCH('[1, 2]')",
		err:   "syntax error at position 34",
	}, {
		input: "SELECT JSON_MERGE_PRESERVE('[1, 2]')",
		err:   "syntax error at position 37",
	}, {
		input: "SELECT JSON_REMOVE('[\"a\", [\"b\", \"c\"], \"d\"]')",
		err:   "syntax error at position 45",
	}, {
		input: "SELECT NTILE('val') OVER w FROM numbers",
		err:   "syntax error at position 19 near 'val'",
	}, {
		input: "SELECT NTILE(-10) OVER w FROM numbers",
		err:   "syntax error at position 15",
	}, {
		input: "SELECT NTH_VALUE(val,) OVER w FROM numbers",
		err:   "syntax error at position 23",
	}, {
		input: "SELECT NTH_VALUE(TRIM('abc'),-10) OVER w FROM numbers",
		err:   "syntax error at position 31",
	}, {
		input: "SELECT LAG(val, ) OVER w",
		err:   "syntax error at position 18",
	}, {
		input: "SELECT LAG(val, 10,  ) OVER w",
		err:   "syntax error at position 23",
	}, {
		input: "SELECT time, subject, val, FIRST_VALUE(val)  OVER w AS 'first', LAST_VALUE(val) OVER w AS 'last', NTH_VALUE(val, 2) OVER w AS 'second', NTH_VALUE(val, 4) OVER w AS 'fourth' FROM observations WINDOW w AS (PARTITION BY subject ORDER BY time ROWS -10 FOLLOWING);",
		err:   "syntax error at position 246",
	}, {
		input: "SELECT BIT_AND(DISTINCT a) FROM products",
		err:   "syntax error at position 24 near 'DISTINCT'",
	}, {
		input: "SELECT BIT_OR(DISTINCT a) FROM products",
		err:   "syntax error at position 23 near 'DISTINCT'",
	}, {
		input: "SELECT BIT_XOR(DISTINCT a) FROM products",
		err:   "syntax error at position 24 near 'DISTINCT'",
	}, {
		input: "SELECT STD(DISTINCT a) FROM products",
		err:   "syntax error at position 20 near 'DISTINCT'",
	}, {
		input: "SELECT STDDEV(DISTINCT a) FROM products",
		err:   "syntax error at position 23 near 'DISTINCT'",
	}, {
		input: "SELECT STDDEV_POP(DISTINCT a) FROM products",
		err:   "syntax error at position 27 near 'DISTINCT'",
	}, {
		input: "SELECT STDDEV_SAMP(DISTINCT a) FROM products",
		err:   "syntax error at position 28 near 'DISTINCT'",
	}, {
		input: "SELECT VAR_POP(DISTINCT a) FROM products",
		err:   "syntax error at position 24 near 'DISTINCT'",
	}, {
		input: "SELECT VAR_SAMP(DISTINCT a) FROM products",
		err:   "syntax error at position 25 near 'DISTINCT'",
	}, {
		input: "SELECT VARIANCE(DISTINCT a) FROM products",
		err:   "syntax error at position 25 near 'DISTINCT'",
	}, {
		input: "SELECT COUNT(DISTINCT *) FROM user",
		err:   "syntax error at position 24",
	}, {
		input: "SELECT x'018' FROM user",
		err:   "syntax error at position 14",
	}, {
		input: "SELECT b'012' FROM user",
		err:   "syntax error at position 12",
	}, {
		input: "SELECT 0b2 FROM user",
		err:   "syntax error at position 11",
	},
	}

	for _, tcase := range invalidSQL {
		t.Run(tcase.input, func(t *testing.T) {
			_, err := Parse(tcase.input)
			require.Error(t, err)
			require.Contains(t, err.Error(), tcase.err)
		})
	}
}

func TestIntroducers(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select _armscii8 'x'",
		output: "select _armscii8 'x' from dual",
	}, {
		input:  "select _ascii 'x'",
		output: "select _ascii 'x' from dual",
	}, {
		input:  "select _big5 'x'",
		output: "select _big5 'x' from dual",
	}, {
		input:  "select _binary 'x'",
		output: "select _binary 'x' from dual",
	}, {
		input:  "select _cp1250 'x'",
		output: "select _cp1250 'x' from dual",
	}, {
		input:  "select _cp1251 'x'",
		output: "select _cp1251 'x' from dual",
	}, {
		input:  "select _cp1256 'x'",
		output: "select _cp1256 'x' from dual",
	}, {
		input:  "select _cp1257 'x'",
		output: "select _cp1257 'x' from dual",
	}, {
		input:  "select _cp850 'x'",
		output: "select _cp850 'x' from dual",
	}, {
		input:  "select _cp852 'x'",
		output: "select _cp852 'x' from dual",
	}, {
		input:  "select _cp866 'x'",
		output: "select _cp866 'x' from dual",
	}, {
		input:  "select _cp932 'x'",
		output: "select _cp932 'x' from dual",
	}, {
		input:  "select _dec8 'x'",
		output: "select _dec8 'x' from dual",
	}, {
		input:  "select _eucjpms 'x'",
		output: "select _eucjpms 'x' from dual",
	}, {
		input:  "select _euckr 'x'",
		output: "select _euckr 'x' from dual",
	}, {
		input:  "select _gb18030 'x'",
		output: "select _gb18030 'x' from dual",
	}, {
		input:  "select _gb2312 'x'",
		output: "select _gb2312 'x' from dual",
	}, {
		input:  "select _gbk 'x'",
		output: "select _gbk 'x' from dual",
	}, {
		input:  "select _geostd8 'x'",
		output: "select _geostd8 'x' from dual",
	}, {
		input:  "select _greek 'x'",
		output: "select _greek 'x' from dual",
	}, {
		input:  "select _hebrew 'x'",
		output: "select _hebrew 'x' from dual",
	}, {
		input:  "select _hp8 'x'",
		output: "select _hp8 'x' from dual",
	}, {
		input:  "select _keybcs2 'x'",
		output: "select _keybcs2 'x' from dual",
	}, {
		input:  "select _koi8r 'x'",
		output: "select _koi8r 'x' from dual",
	}, {
		input:  "select _koi8u 'x'",
		output: "select _koi8u 'x' from dual",
	}, {
		input:  "select _latin1 'x'",
		output: "select _latin1 'x' from dual",
	}, {
		input:  "select _latin2 'x'",
		output: "select _latin2 'x' from dual",
	}, {
		input:  "select _latin5 'x'",
		output: "select _latin5 'x' from dual",
	}, {
		input:  "select _latin7 'x'",
		output: "select _latin7 'x' from dual",
	}, {
		input:  "select _macce 'x'",
		output: "select _macce 'x' from dual",
	}, {
		input:  "select _macroman 'x'",
		output: "select _macroman 'x' from dual",
	}, {
		input:  "select _sjis 'x'",
		output: "select _sjis 'x' from dual",
	}, {
		input:  "select _swe7 'x'",
		output: "select _swe7 'x' from dual",
	}, {
		input:  "select _tis620 'x'",
		output: "select _tis620 'x' from dual",
	}, {
		input:  "select _ucs2 'x'",
		output: "select _ucs2 'x' from dual",
	}, {
		input:  "select _ujis 'x'",
		output: "select _ujis 'x' from dual",
	}, {
		input:  "select _utf16 'x'",
		output: "select _utf16 'x' from dual",
	}, {
		input:  "select _utf16le 'x'",
		output: "select _utf16le 'x' from dual",
	}, {
		input:  "select _utf32 'x'",
		output: "select _utf32 'x' from dual",
	}, {
		input:  "select _utf8 'x'",
		output: "select _utf8 'x' from dual",
	}, {
		input:  "select _utf8mb4 'x'",
		output: "select _utf8mb4 'x' from dual",
	}, {
		input:  "select _utf8mb3 'x'",
		output: "select _utf8 'x' from dual",
	}}
	for _, tcase := range validSQL {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.output == "" {
				tcase.output = tcase.input
			}
			tree, err := Parse(tcase.input)
			assert.NoError(t, err)
			out := String(tree)
			assert.Equal(t, tcase.output, out)
		})
	}
}

func TestCaseSensitivity(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "create table A (\n\t`B` int\n)",
		output: "create table A (\n\tB int\n)",
	}, {
		input:  "create index b on A (col1 desc)",
		output: "alter table A add index b (col1 desc)",
	}, {
		input:  "alter table A foo",
		output: "alter table A",
	}, {
		input:  "alter table A convert unparsable",
		output: "alter table A",
	}, {
		input: "alter view A as select * from t",
	}, {
		input:  "alter table A rename to B",
		output: "alter table A rename B",
	}, {
		input:  "alter table `A r` rename to `B r`",
		output: "alter table `A r` rename `B r`",
	}, {
		input: "rename table A to B",
	}, {
		input:  "drop table B",
		output: "drop table B",
	}, {
		input:  "drop table if exists B",
		output: "drop table if exists B",
	}, {
		input:  "drop index b on A",
		output: "alter table A drop key b",
	}, {
		input: "select a from B",
	}, {
		input: "select A as B from C",
	}, {
		input: "select B.* from c",
	}, {
		input: "select B.A from c",
	}, {
		input: "select * from B as C",
	}, {
		input: "select * from A.B",
	}, {
		input: "update A set b = 1",
	}, {
		input: "update A.B set b = 1",
	}, {
		input: "select A() from b",
	}, {
		input: "select A(B, C) from b",
	}, {
		// IF is an exception. It's always lower-cased.
		input:  "select IF(B, C) from b",
		output: "select if(B, C) from b",
	}, {
		input: "select * from b use index (A)",
	}, {
		input: "insert into A(A, B) values (1, 2)",
	}, {
		input:  "CREATE TABLE A (\n\t`A` int\n)",
		output: "create table A (\n\tA int\n)",
	}, {
		input: "create view A as select * from b",
	}, {
		input: "drop view A",
	}, {
		input: "drop view if exists A",
	}, {
		input:  "select /* lock in SHARE MODE */ 1 from t lock in SHARE MODE",
		output: "select /* lock in SHARE MODE */ 1 from t lock in share mode",
	}, {
		input:  "select next VALUE from t",
		output: "select next 1 values from t",
	}, {
		input: "select /* use */ 1 from t1 use index (A) where b = 1",
	}}
	for _, tcase := range validSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		out := String(tree)
		if out != tcase.output {
			t.Errorf("out: %s, want %s", out, tcase.output)
		}
	}
}

func TestKeywords(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select current_timestamp",
		output: "select current_timestamp() from dual",
	}, {
		input: "update t set a = current_timestamp()",
	}, {
		input: "update t set a = current_timestamp(5)",
	}, {
		input:  "select a, current_date from t",
		output: "select a, current_date() from t",
	}, {
		input:  "insert into t(a, b) values (current_date, current_date())",
		output: "insert into t(a, b) values (current_date(), current_date())",
	}, {
		input: "select * from t where a > utc_timestmp()",
	}, {
		input: "select * from t where a > utc_timestamp(4)",
	}, {
		input:  "update t set b = utc_timestamp + 5",
		output: "update t set b = utc_timestamp() + 5",
	}, {
		input:  "select utc_time, utc_date, utc_time(6)",
		output: "select utc_time(), utc_date(), utc_time(6) from dual",
	}, {
		input:  "select 1 from dual where localtime > utc_time",
		output: "select 1 from dual where localtime() > utc_time()",
	}, {
		input:  "select 1 from dual where localtime(2) > utc_time(1)",
		output: "select 1 from dual where localtime(2) > utc_time(1)",
	}, {
		input:  "update t set a = localtimestamp(), b = utc_timestamp",
		output: "update t set a = localtimestamp(), b = utc_timestamp()",
	}, {
		input:  "update t set a = localtimestamp(10), b = utc_timestamp(13)",
		output: "update t set a = localtimestamp(10), b = utc_timestamp(13)",
	}, {
		input: "insert into t(a) values (unix_timestamp)",
	}, {
		input: "select replace(a, 'foo', 'bar') from t",
	}, {
		input: "update t set a = replace('1234', '2', '1')",
	}, {
		input: "insert into t(a, b) values ('foo', 'bar') on duplicate key update a = replace(hex('foo'), 'f', 'b')",
	}, {
		input: "update t set a = left('1234', 3)",
	}, {
		input: "select left(a, 5) from t",
	}, {
		input: "update t set d = adddate(date('2003-12-31 01:02:03'), interval 5 day)",
	}, {
		input: "insert into t(a, b) values (left('foo', 1), 'b')",
	}, {
		input: "insert /* qualified function */ into t(a, b) values (test.PI(), 'b')",
	}, {
		input:  "select /* keyword in qualified id */ * from t join z on t.key = z.key",
		output: "select /* keyword in qualified id */ * from t join z on t.`key` = z.`key`",
	}, {
		input:  "select /* non-reserved keywords as unqualified cols */ date, view, offset from t",
		output: "select /* non-reserved keywords as unqualified cols */ `date`, `view`, `offset` from t",
	}, {
		input:  "select /* share and mode as cols */ share, mode from t where share = 'foo'",
		output: "select /* share and mode as cols */ `share`, `mode` from t where `share` = 'foo'",
	}, {
		input:  "select status from t",
		output: "select `status` from t",
	}, {
		input:  "select Status from t",
		output: "select `Status` from t",
	}, {
		input:  "select variables from t",
		output: "select `variables` from t",
	}, {
		input:  "select Variables from t",
		output: "select `Variables` from t",
	}, {
		input:  "select current_user, current_user() from dual",
		output: "select current_user(), current_user() from dual",
	}}

	for _, tcase := range validSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		out := String(tree)
		if out != tcase.output {
			t.Errorf("out: %s, want %s", out, tcase.output)
		}
	}
}

func TestConvert(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input: "select cast('abc' as date) from t",
	}, {
		input: "select convert('abc', binary(4)) from t",
	}, {
		input: "select convert('abc', binary) from t",
	}, {
		input: "select convert('abc', char character set binary) from t",
	}, {
		input:  "select convert('abc', char(4) ascii) from t",
		output: "select convert('abc', char(4) character set latin1) from t",
	}, {
		input:  "select convert('abc', char(4) ascii binary) from t",
		output: "select convert('abc', char(4) character set latin1 binary) from t",
	}, {
		input:  "select convert('abc', char unicode) from t",
		output: "select convert('abc', char character set ucs2) from t",
	}, {
		input: "select convert('abc', char(4)) from t",
	}, {
		input: "select convert('abc', char) from t",
	}, {
		input: "select convert('abc', nchar(4)) from t",
	}, {
		input: "select convert('abc', nchar) from t",
	}, {
		input: "select convert('abc', signed) from t",
	}, {
		input:  "select convert('abc', signed integer) from t",
		output: "select convert('abc', signed) from t",
	}, {
		input: "select convert('abc', unsigned) from t",
	}, {
		input:  "select convert('abc', unsigned integer) from t",
		output: "select convert('abc', unsigned) from t",
	}, {
		input: "select convert('abc', decimal(3, 4)) from t",
	}, {
		input: "select convert('abc', decimal(4)) from t",
	}, {
		input: "select convert('abc', decimal) from t",
	}, {
		input: "select convert('abc', date) from t",
	}, {
		input: "select convert('abc', time(4)) from t",
	}, {
		input: "select convert('abc', time) from t",
	}, {
		input: "select convert('abc', datetime(9)) from t",
	}, {
		input: "select convert('abc', datetime) from t",
	}, {
		input: "select convert('abc', json) from t",
	}, {
		input: "select cast(json_keys(c) as char(64) array) from t",
	}}

	for _, tcase := range validSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		out := String(tree)
		if out != tcase.output {
			t.Errorf("out: %s, want %s", out, tcase.output)
		}
	}

	invalidSQL := []struct {
		input  string
		output string
	}{{
		input:  "select convert('abc' as date) from t",
		output: "syntax error at position 24 near 'as'",
	}, {
		input:  "select convert from t",
		output: "syntax error at position 20 near 'from'",
	}, {
		input:  "select cast('foo', decimal) from t",
		output: "syntax error at position 19",
	}, {
		input:  "select convert('abc', datetime(4+9)) from t",
		output: "syntax error at position 34",
	}, {
		input:  "select convert('abc', decimal(4+9)) from t",
		output: "syntax error at position 33",
	}, {
		input:  "set transaction isolation level 12345",
		output: "syntax error at position 38 near '12345'",
	}, {
		input:  "@",
		output: "syntax error at position 2",
	}, {
		input:  "@@",
		output: "syntax error at position 3",
	}, {
		input:  "select A(distinct B, C) from b",
		output: "syntax error at position 18 near 'distinct'",
	}}

	for _, tcase := range invalidSQL {
		_, err := Parse(tcase.input)
		if err == nil || err.Error() != tcase.output {
			t.Errorf("%s: %v, want %s", tcase.input, err, tcase.output)
		}
	}
}

func TestSelectInto(t *testing.T) {
	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select * from t order by name limit 100 into outfile s3 'out_file_name'",
		output: "select * from t order by `name` asc limit 100 into outfile s3 'out_file_name'",
	}, {
		input: `select * from TestPerson into outfile s3 's3://test-bucket/export_import/export/users.csv' fields terminated by ',' enclosed by '\"' escaped by '\\' overwrite on`,
	}, {
		input: "select * from t into dumpfile 'out_file_name'",
	}, {
		input: "select * from t into outfile 'out_file_name' character set binary fields terminated by 'term' optionally enclosed by 'c' escaped by 'e' lines starting by 'a' terminated by '\\n'",
	}, {
		input: "select * from t into outfile s3 'out_file_name' character set binary format csv header fields terminated by 'term' optionally enclosed by 'c' escaped by 'e' lines starting by 'a' terminated by '\\n' manifest on overwrite off",
	}, {
		input: "select * from t into outfile s3 'out_file_name' character set binary lines terminated by '\\n' starting by 'a' manifest on overwrite off",
	}, {
		input:  "select * from (select * from t union select * from t2) as t3 where t3.name in (select col from t4) into outfile s3 'out_file_name'",
		output: "select * from (select * from t union select * from t2) as t3 where t3.`name` in (select col from t4) into outfile s3 'out_file_name'",
	}, {
		input: `select * from TestPerson into outfile s3 's3://test-bucket/export_import/export/users.csv' character set 'utf8' overwrite on`,
	}, {
		input: `select * from t1 into outfile '/tmp/foo.csv' fields escaped by '\\' terminated by '\n'`,
	}, {
		input: `select * from t1 into outfile '/tmp/foo.csv' fields escaped by 'c' terminated by '\n' enclosed by '\t'`,
	}, {
		input:  `alter vschema create vindex my_vdx using hash`,
		output: "alter vschema create vindex my_vdx using `hash`",
	}}

	for _, tcase := range validSQL {
		t.Run(tcase.input, func(t *testing.T) {
			if tcase.output == "" {
				tcase.output = tcase.input
			}
			tree, err := Parse(tcase.input)
			require.NoError(t, err)
			out := String(tree)
			assert.Equal(t, tcase.output, out)
		})
	}

	invalidSQL := []struct {
		input  string
		output string
	}{{
		input:  "select convert('abc' as date) from t",
		output: "syntax error at position 24 near 'as'",
	}, {
		input:  "set transaction isolation level 12345",
		output: "syntax error at position 38 near '12345'",
	}}

	for _, tcase := range invalidSQL {
		_, err := Parse(tcase.input)
		if err == nil || err.Error() != tcase.output {
			t.Errorf("%s: %v, want %s", tcase.input, err, tcase.output)
		}
	}
}

func TestPositionedErr(t *testing.T) {
	invalidSQL := []struct {
		input  string
		output PositionedErr
	}{{
		input:  "select convert('abc' as date) from t",
		output: PositionedErr{"syntax error", 24, "as"},
	}, {
		input:  "select convert from t",
		output: PositionedErr{"syntax error", 20, "from"},
	}, {
		input:  "select cast('foo', decimal) from t",
		output: PositionedErr{"syntax error", 19, ""},
	}, {
		input:  "select convert('abc', datetime(4+9)) from t",
		output: PositionedErr{"syntax error", 34, ""},
	}, {
		input:  "select convert('abc', decimal(4+9)) from t",
		output: PositionedErr{"syntax error", 33, ""},
	}, {
		input:  "set transaction isolation level 12345",
		output: PositionedErr{"syntax error", 38, "12345"},
	}, {
		input:  "select * from a left join b",
		output: PositionedErr{"syntax error", 28, ""},
	}, {
		input:  "select a from (select * from tbl)",
		output: PositionedErr{"syntax error", 34, ""},
	}}

	for _, tcase := range invalidSQL {
		tkn := NewStringTokenizer(tcase.input)
		_, err := ParseNext(tkn)

		if posErr, ok := err.(PositionedErr); !ok {
			t.Errorf("%s: %v expected PositionedErr, got (%T) %v", tcase.input, err, err, tcase.output)
		} else if posErr.Pos != tcase.output.Pos || posErr.Near != tcase.output.Near || err.Error() != tcase.output.Error() {
			t.Errorf("%s: %v, want: %v", tcase.input, err, tcase.output)
		}
	}
}

func TestSubStr(t *testing.T) {

	validSQL := []struct {
		input  string
		output string
	}{{
		input: `select substr('foobar', 1) from t`,
	}, {
		input: "select substr(a, 1, 6) from t",
	}, {
		input:  "select substring(a, 1) from t",
		output: "select substr(a, 1) from t",
	}, {
		input:  "select substring(a, 1, 6) from t",
		output: "select substr(a, 1, 6) from t",
	}, {
		input:  "select substr(a from 1 for 6) from t",
		output: "select substr(a, 1, 6) from t",
	}, {
		input:  "select substring(a from 1 for 6) from t",
		output: "select substr(a, 1, 6) from t",
	}, {
		input:  `select substr("foo" from 1 for 2) from t`,
		output: `select substr('foo', 1, 2) from t`,
	}, {
		input:  `select substring("foo", 1, 2) from t`,
		output: `select substr('foo', 1, 2) from t`,
	}, {
		input:  `select substr(substr("foo" from 1 for 2), 1, 2) from t`,
		output: `select substr(substr('foo', 1, 2), 1, 2) from t`,
	}, {
		input:  `select substr(substring("foo", 1, 2), 3, 4) from t`,
		output: `select substr(substr('foo', 1, 2), 3, 4) from t`,
	}, {
		input:  `select substring(substr("foo", 1), 2) from t`,
		output: `select substr(substr('foo', 1), 2) from t`,
	}}

	for _, tcase := range validSQL {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		out := String(tree)
		if out != tcase.output {
			t.Errorf("out: %s, want %s", out, tcase.output)
		}
	}
}

func TestLoadData(t *testing.T) {
	validSQL := []string{
		"load data from s3 'x.txt'",
		"load data from s3 manifest 'x.txt'",
		"load data from s3 file 'x.txt'",
		"load data infile 'x.txt' into table 'c'",
		"load data from s3 'x.txt' into table x"}

	for _, tcase := range validSQL {
		_, err := Parse(tcase)
		require.NoError(t, err)
	}
}

func TestCreateTable(t *testing.T) {
	createTableQueries := []struct {
		input, output string
	}{{
		// test all the data types and options
		input: `create table t (
	col_bit bit,
	col_tinyint tinyint auto_increment,
	col_tinyint3 tinyint(3) unsigned,
	col_smallint smallint,
	col_smallint4 smallint(4) zerofill,
	col_mediumint mediumint,
	col_mediumint5 mediumint(5) unsigned not null,
	col_int int,
	col_int10 int(10) not null,
	col_integer integer comment 'this is an integer',
	col_bigint bigint,
	col_bigint10 bigint(10) zerofill not null default 10,
	col_real real,
	col_real2 real(1,2) not null default 1.23,
	col_double double,
	col_double2 double(3,4) not null default 1.23,
	col_float float,
	col_float2 float(3,4) not null default 1.23,
	col_decimal decimal,
	col_decimal2 decimal(2),
	col_decimal3 decimal(2,3),
	col_numeric numeric,
	col_numeric2 numeric(2),
	col_numeric3 numeric(2,3),
	col_date date,
	col_time time,
	col_timestamp timestamp,
	col_datetime datetime,
	col_year year,
	col_char char,
	col_char2 char(2),
	col_char3 char(3) character set ascii,
	col_char4 char(4) character set ascii collate ascii_bin,
	col_varchar varchar,
	col_varchar2 varchar(2),
	col_varchar3 varchar(3) character set ascii,
	col_varchar4 varchar(4) character set ascii collate ascii_bin,
	col_binary binary,
	col_varbinary varbinary(10),
	col_tinyblob tinyblob,
	col_blob blob,
	col_mediumblob mediumblob,
	col_longblob longblob,
	col_tinytext tinytext,
	col_text text,
	col_mediumtext mediumtext,
	col_longtext longtext,
	col_text text character set ascii collate ascii_bin,
	col_json json,
	col_enum enum('a', 'b', 'c', 'd'),
	col_enum2 enum('a', 'b', 'c', 'd') character set ascii,
	col_enum3 enum('a', 'b', 'c', 'd') collate ascii_bin,
	col_enum4 enum('a', 'b', 'c', 'd') character set ascii collate ascii_bin,
	col_set set('a', 'b', 'c', 'd'),
	col_set2 set('a', 'b', 'c', 'd') character set ascii,
	col_set3 set('a', 'b', 'c', 'd') collate ascii_bin,
	col_set4 set('a', 'b', 'c', 'd') character set ascii collate ascii_bin,
	col_geometry1 geometry,
	col_geometry2 geometry not null,
	col_point1 point,
	col_point2 point not null,
	col_linestring1 linestring,
	col_linestring2 linestring not null,
	col_polygon1 polygon,
	col_polygon2 polygon not null,
	col_geometrycollection1 geometrycollection,
	col_geometrycollection2 geometrycollection not null,
	col_multipoint1 multipoint,
	col_multipoint2 multipoint not null,
	col_multilinestring1 multilinestring,
	col_multilinestring2 multilinestring not null,
	col_multipolygon1 multipolygon,
	col_multipolygon2 multipolygon not null
)`,
	},
		// test null columns
		{
			input: `create table foo (
	id int primary key,
	a varchar(255) null,
	b varchar(255) null default 'foo',
	c timestamp null default current_timestamp()
)`,
		},
		// test defining indexes separately
		{
			input: `create table t (
	id int auto_increment,
	username varchar,
	email varchar,
	full_name varchar,
	geom point not null,
	status_nonkeyword varchar,
	primary key (id),
	spatial key geom (geom),
	fulltext key fts (full_name),
	unique key by_username (username),
	unique key by_username2 (username),
	unique index by_username3 (username),
	index by_status (status_nonkeyword),
	key by_full_name (full_name)
)`,
		},
		// test defining index visibility
		{
			input: `create table t (
	id int auto_increment,
	username varchar,
	unique key by_username (username) visible,
	unique key by_username2 (username) invisible,
	unique index by_username3 (username)
)`,
		},
		// test adding engine attributes
		{
			input: `create table t (
	id int auto_increment,
	username varchar,
	unique key by_username (username) engine_attribute '{}' secondary_engine_attribute '{}',
	unique index by_username3 (username)
)`,
		},
		// test defining SRID
		{
			input: `create table t (
	p point srid 0,
	g geometry not null srid 4326
)`,
		},
		// test defining column visibility
		{
			input: `create table t (
	id int auto_increment,
	username varchar invisible,
	login varchar visible
)`,
		},
		// test adding column engine attributes
		{
			input: `create table t (
	id int auto_increment,
	username varchar engine_attribute '{}' secondary_engine_attribute '{}'
)`,
		},
		// test adding column format
		{
			input: `create table t (
	id int auto_increment,
	username varchar column_format dynamic
)`,
		},
		// test that indexes support USING <id>
		{
			input: `create table t (
	id int auto_increment,
	username varchar,
	email varchar,
	full_name varchar,
	status_nonkeyword varchar,
	primary key (id) using BTREE,
	unique key by_username (username) using HASH,
	unique key by_username2 (username) using OTHER,
	unique index by_username3 (username) using XYZ,
	index by_status (status_nonkeyword) using PDQ,
	key by_full_name (full_name) using OTHER
)`,
		},
		// test other index options
		{
			input: `create table t (
	id int auto_increment,
	username varchar,
	email varchar,
	primary key (id) comment 'hi',
	unique key by_username (username) key_block_size 8,
	unique index by_username4 (username) comment 'hi' using BTREE,
	unique index by_username4 (username) using BTREE key_block_size 4 comment 'hi'
)`,
		},
		{
			input: `create table t1 (
	first_name varchar(10),
	last_name varchar(10),
	full_name varchar(255) character set utf8mb4 collate utf8mb4_0900_ai_ci as (concat(first_name, ' ', last_name)) virtual,
	middle_name varchar(255) character set utf8mb4 collate utf8mb4_0900_ai_ci default '' collate utf8mb4_bin
)`,
			output: `create table t1 (
	first_name varchar(10),
	last_name varchar(10),
	full_name varchar(255) character set utf8mb4 collate utf8mb4_0900_ai_ci as (concat(first_name, ' ', last_name)) virtual,
	middle_name varchar(255) character set utf8mb4 collate utf8mb4_bin default ''
)`,
		},
		{
			input: "create table t1 (`idb` varchar(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci GENERATED ALWAYS AS (json_unquote(json_extract(`jsonobj`,_utf8mb4'$._id'))) STORED NOT NULL)",
			output: `create table t1 (
	idb varchar(36) character set utf8mb4 collate utf8mb4_0900_ai_ci as (json_unquote(json_extract(jsonobj, _utf8mb4 '$._id'))) stored not null
)`,
		},
		{
			input: "create table t2 (\n\tid int not null,\n\textra tinyint(1) as (id = 1) stored,\n\tPRIMARY KEY (id)\n)",
		},
		// multi-column indexes
		{
			input: `create table t (
	id int auto_increment,
	username varchar,
	email varchar,
	full_name varchar,
	a int,
	b int,
	c int,
	primary key (id, username),
	unique key by_abc (a, b, c),
	unique key (a, b, c),
	key by_email (email(10), username)
)`,
		},
		// geometrycollection & geomcollection alias
		{
			input: `create table t (
	id int auto_increment,
	col_geometrycollection1 geometrycollection,
	col_geometrycollection2 geometrycollection not null,
	col_geometrycollection3 geomcollection,
	col_geometrycollection4 geomcollection not null,
	primary key (id)
)`,
		},
		// foreign keys
		{
			input: `create table t (
	id int auto_increment,
	username varchar,
	k int,
	Z int,
	newCol int references t2 (a),
	newCol int references t2 (a) on delete restrict,
	newCol int references t2 (a) on delete no action,
	newCol int references t2 (a) on delete cascade on update set default,
	newCol int references t2 (a) on delete set default on update set null,
	newCol int references t2 (a) on delete set null on update restrict,
	newCol int references t2 (a) on update set default on delete cascade,
	newCol int references t2 (a) on update set null on delete set default,
	newCol int references t2 (a) on update restrict on delete set null,
	newCol int references t2 (a) match full on delete cascade on update set default,
	newCol int references t2 (a) match partial on delete set default on update set null,
	newCol int references t2 (a) match simple on delete set null on update restrict,
	newCol int references t2 (a) on update no action,
	newCol int references t2 (a) on update cascade,
	primary key (id, username),
	key by_email (email(10), username),
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b),
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete restrict,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete no action,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete cascade on update set default,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete set default on update set null,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete set null on update restrict,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on update set default on delete cascade ,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on update set null on delete set default,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on update restrict on delete set null,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) match full on delete cascade on update set default,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) match partial on delete set default on update set null,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) match simple on delete set null on update restrict,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on update no action,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on update cascade
)`,
			output: `create table t (
	id int auto_increment,
	username varchar,
	k int,
	Z int,
	newCol int references t2 (a),
	newCol int references t2 (a) on delete restrict,
	newCol int references t2 (a) on delete no action,
	newCol int references t2 (a) on delete cascade on update set default,
	newCol int references t2 (a) on delete set default on update set null,
	newCol int references t2 (a) on delete set null on update restrict,
	newCol int references t2 (a) on delete cascade on update set default,
	newCol int references t2 (a) on delete set default on update set null,
	newCol int references t2 (a) on delete set null on update restrict,
	newCol int references t2 (a) match full on delete cascade on update set default,
	newCol int references t2 (a) match partial on delete set default on update set null,
	newCol int references t2 (a) match simple on delete set null on update restrict,
	newCol int references t2 (a) on update no action,
	newCol int references t2 (a) on update cascade,
	primary key (id, username),
	key by_email (email(10), username),
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b),
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete restrict,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete no action,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete cascade on update set default,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete set default on update set null,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete set null on update restrict,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete cascade on update set default,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete set default on update set null,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on delete set null on update restrict,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) match full on delete cascade on update set default,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) match partial on delete set default on update set null,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) match simple on delete set null on update restrict,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on update no action,
	constraint second_ibfk_1 foreign key (k, j) references t2 (a, b) on update cascade
)`,
		},
		// constraint name with spaces
		{
			input: `create table ` + "`" + `Post With Space` + "`" + ` (
	id int(11) not null auto_increment,
	user_id int(11) not null,
	primary key (id),
	unique key post_user_unique (user_id),
	constraint ` + "`" + `Post With Space_ibfk_1` + "`" + ` foreign key (user_id) references ` + "`" + `User` + "`" + ` (id)
) ENGINE Innodb`,
		},
		// table options
		{
			input: `create table t (
	id int auto_increment
) engine InnoDB,
  auto_increment 123,
  autoextend_size 16,
  avg_row_length 1,
  charset utf8mb4,
  charset latin1,
  checksum 0,
  collate binary,
  collate ascii_bin,
  comment 'this is a comment',
  compression 'zlib',
  connection 'connect_string',
  data directory 'absolute path to directory',
  delay_key_write 1,
  encryption 'n',
  engine_attribute '{}',
  index directory 'absolute path to directory',
  insert_method no,
  key_block_size 1024,
  max_rows 100,
  min_rows 10,
  pack_keys 0,
  password 'sekret',
  row_format default,
  secondary_engine_attribute '{}',
  stats_auto_recalc default,
  stats_persistent 0,
  stats_sample_pages 1,
  tablespace tablespace_name storage disk,
  union (a, b, c),
  tablespace tablespace_name`,
		},
		// boolean columns
		{
			input: `create table t (
	bi bigint not null primary key,
	b1 bool not null,
	b2 boolean
)`,
		},
		{
			// test key_block_size
			input: `create table t (
	id int auto_increment,
	username varchar,
	unique key by_username (username) key_block_size 8,
	unique key by_username2 (username) key_block_size=8,
	unique by_username3 (username) key_block_size = 4
)`,
			output: `create table t (
	id int auto_increment,
	username varchar,
	unique key by_username (username) key_block_size 8,
	unique key by_username2 (username) key_block_size 8,
	unique key by_username3 (username) key_block_size 4
)`,
		}, {
			// test defaults
			input: `create table t (
	i1 int default 1,
	i2 int default null,
	f1 float default 1.23,
	s1 varchar default 'c',
	s2 varchar default 'this is a string',
	s3 varchar default null,
	s4 timestamp default current_timestamp,
	s41 timestamp default now,
	s5 bit(1) default B'0'
)`,
			output: `create table t (
	i1 int default 1,
	i2 int default null,
	f1 float default 1.23,
	s1 varchar default 'c',
	s2 varchar default 'this is a string',
	` + "`" + `s3` + "`" + ` varchar default null,
	s4 timestamp default current_timestamp(),
	s41 timestamp default now(),
	s5 bit(1) default B'0'
)`,
		}, {
			// test non_reserved word in column name
			input: `create table t (
	repair int
)`,
			output: `create table t (
	` + "`" + `repair` + "`" + ` int
)`,
		}, {
			// test key field options
			input: `create table t (
	id int auto_increment primary key,
	username varchar unique key,
	email varchar unique,
	full_name varchar key,
	time1 timestamp on update current_timestamp,
	time2 timestamp default current_timestamp on update current_timestamp
)`,
			output: `create table t (
	id int auto_increment primary key,
	username varchar unique key,
	email varchar unique,
	full_name varchar key,
	time1 timestamp on update current_timestamp(),
	time2 timestamp default current_timestamp() on update current_timestamp()
)`,
		}, {
			// test current_timestamp with and without ()
			input: `create table t (
	time1 timestamp default current_timestamp,
	time2 timestamp default current_timestamp(),
	time3 timestamp default current_timestamp on update current_timestamp,
	time4 timestamp default current_timestamp() on update current_timestamp(),
	time5 timestamp(3) default current_timestamp(3) on update current_timestamp(3)
)`,
			output: `create table t (
	time1 timestamp default current_timestamp(),
	time2 timestamp default current_timestamp(),
	time3 timestamp default current_timestamp() on update current_timestamp(),
	time4 timestamp default current_timestamp() on update current_timestamp(),
	time5 timestamp(3) default current_timestamp(3) on update current_timestamp(3)
)`,
		}, {
			// test now with and without ()
			input: `create table t (
	time1 timestamp default now,
	time2 timestamp default now(),
	time3 timestamp default (now()),
	time4 timestamp default now on update now,
	time5 timestamp default now() on update now(),
	time6 timestamp(3) default now(3) on update now(3)
)`,
			output: `create table t (
	time1 timestamp default now(),
	time2 timestamp default now(),
	time3 timestamp default now(),
	time4 timestamp default now() on update now(),
	time5 timestamp default now() on update now(),
	time6 timestamp(3) default now(3) on update now(3)
)`,
		}, {
			// test localtime with and without ()
			input: `create table t (
	time1 timestamp default localtime,
	time2 timestamp default localtime(),
	time3 timestamp default localtime on update localtime,
	time4 timestamp default localtime() on update localtime(),
	time5 timestamp(6) default localtime(6) on update localtime(6)
)`,
			output: `create table t (
	time1 timestamp default localtime(),
	time2 timestamp default localtime(),
	time3 timestamp default localtime() on update localtime(),
	time4 timestamp default localtime() on update localtime(),
	time5 timestamp(6) default localtime(6) on update localtime(6)
)`,
		}, {
			// test localtimestamp with and without ()
			input: `create table t (
	time1 timestamp default localtimestamp,
	time2 timestamp default localtimestamp(),
	time3 timestamp default localtimestamp on update localtimestamp,
	time4 timestamp default localtimestamp() on update localtimestamp(),
	time5 timestamp(1) default localtimestamp(1) on update localtimestamp(1)
)`,
			output: `create table t (
	time1 timestamp default localtimestamp(),
	time2 timestamp default localtimestamp(),
	time3 timestamp default localtimestamp() on update localtimestamp(),
	time4 timestamp default localtimestamp() on update localtimestamp(),
	time5 timestamp(1) default localtimestamp(1) on update localtimestamp(1)
)`,
		}, {
			input: `create table t1 (
	first_name varchar(10),
	last_name varchar(10),
	full_name varchar(255) as (concat(first_name, ' ', last_name))
)`, output: `create table t1 (
	first_name varchar(10),
	last_name varchar(10),
	full_name varchar(255) as (concat(first_name, ' ', last_name)) virtual
)`,
		}, {
			input: `create table t1 (id int, gnrtd int as (id+2) virtual)`,
			output: `create table t1 (
	id int,
	gnrtd int as (id + 2) virtual
)`,
		}, {
			input: `create table t1 (first_name varchar(10), last_name varchar(10),
	full_name varchar(255) generated always as (concat(first_name, ' ', last_name)))`,
			output: `create table t1 (
	first_name varchar(10),
	last_name varchar(10),
	full_name varchar(255) as (concat(first_name, ' ', last_name)) virtual
)`,
		}, {
			input: `create table t1 (first_name varchar(10), last_name varchar(10),
	full_name varchar(255) generated always as (concat(first_name, ' ', last_name)) not null )`,
			output: `create table t1 (
	first_name varchar(10),
	last_name varchar(10),
	full_name varchar(255) as (concat(first_name, ' ', last_name)) virtual not null
)`,
		}, {
			input: `create table t1 (first_name varchar(10), full_name varchar(255) as (concat(first_name, ' ', last_name)) null)`,
			output: `create table t1 (
	first_name varchar(10),
	full_name varchar(255) as (concat(first_name, ' ', last_name)) virtual null
)`,
		}, {
			input: `create table t1 (first_name varchar(10), full_name varchar(255) as (concat(first_name, ' ', last_name)) unique)`,
			output: `create table t1 (
	first_name varchar(10),
	full_name varchar(255) as (concat(first_name, ' ', last_name)) virtual unique
)`,
		}, {
			input: `create table t1 (first_name varchar(10), full_name varchar(255) as (concat(first_name, ' ', last_name)) unique key)`,
			output: `create table t1 (
	first_name varchar(10),
	full_name varchar(255) as (concat(first_name, ' ', last_name)) virtual unique key
)`,
		}, {
			input: `create table t1 (first_name varchar(10), full_name varchar(255) as (concat(first_name, ' ', last_name)) key)`,
			output: `create table t1 (
	first_name varchar(10),
	full_name varchar(255) as (concat(first_name, ' ', last_name)) virtual key
)`,
		}, {
			input: `create table t1 (first_name varchar(10), full_name varchar(255) as (concat(first_name, ' ', last_name)) primary key)`,
			output: `create table t1 (
	first_name varchar(10),
	full_name varchar(255) as (concat(first_name, ' ', last_name)) virtual primary key
)`,
		}, {
			input: `create table t1 (first_name varchar(10), full_name varchar(255) as (concat(first_name, ' ', last_name)) comment 'hello world')`,
			output: `create table t1 (
	first_name varchar(10),
	full_name varchar(255) as (concat(first_name, ' ', last_name)) virtual comment 'hello world'
)`,
		}, {
			input: `create table non_reserved_keyword (id int(11)) ENGINE = MEMORY`,
			output: `create table non_reserved_keyword (
	id int(11)
) ENGINE MEMORY`,
		}, {
			input: `create table non_reserved_keyword (id int(11)) ENGINE = MEDIUMTEXT`,
			output: `create table non_reserved_keyword (
	id int(11)
) ENGINE MEDIUMTEXT`,
		}, {
			input: `create table t1 (id int(11)) ENGINE = FOOBAR`,
			output: `create table t1 (
	id int(11)
) ENGINE FOOBAR`,
		},
		// partitions
		{
			input: `
CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL DEFAULT '1970-01-01',
    separated DATE NOT NULL DEFAULT '9999-12-31',
    job_code INT NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY RANGE (store_id) (
    PARTITION p0 VALUES LESS THAN (6),
    PARTITION p1 VALUES LESS THAN (11),
    PARTITION p2 VALUES LESS THAN (16),
    PARTITION p3 VALUES LESS THAN (21)
)`,
			output: `create table employees (
	id INT not null,
	fname VARCHAR(30),
	lname VARCHAR(30),
	hired DATE not null default '1970-01-01',
	separated DATE not null default '9999-12-31',
	job_code INT not null,
	store_id INT not null
)
partition by range (store_id)
(partition p0 values less than (6),
 partition p1 values less than (11),
 partition p2 values less than (16),
 partition p3 values less than (21))`,
		},
		{
			input: `CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL DEFAULT '1970-01-01',
    separated DATE NOT NULL DEFAULT '9999-12-31',
    job_code INT NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY RANGE (store_id) (
    PARTITION p0 VALUES LESS THAN (6),
    PARTITION p1 VALUES LESS THAN (11),
    PARTITION p2 VALUES LESS THAN (16),
    PARTITION p3 VALUES LESS THAN MAXVALUE
)`,
			output: `create table employees (
	id INT not null,
	fname VARCHAR(30),
	lname VARCHAR(30),
	hired DATE not null default '1970-01-01',
	separated DATE not null default '9999-12-31',
	job_code INT not null,
	store_id INT not null
)
partition by range (store_id)
(partition p0 values less than (6),
 partition p1 values less than (11),
 partition p2 values less than (16),
 partition p3 values less than maxvalue)`,
		},
		{
			input: `CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL DEFAULT '1970-01-01',
    separated DATE NOT NULL DEFAULT '9999-12-31',
    job_code INT NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY RANGE (job_code) (
    PARTITION p0 VALUES LESS THAN (100),
    PARTITION p1 VALUES LESS THAN (1000),
    PARTITION p2 VALUES LESS THAN (10000)
)`,
			output: `create table employees (
	id INT not null,
	fname VARCHAR(30),
	lname VARCHAR(30),
	hired DATE not null default '1970-01-01',
	separated DATE not null default '9999-12-31',
	job_code INT not null,
	store_id INT not null
)
partition by range (job_code)
(partition p0 values less than (100),
 partition p1 values less than (1000),
 partition p2 values less than (10000))`,
		},
		{
			input: `CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL DEFAULT '1970-01-01',
    separated DATE NOT NULL DEFAULT '9999-12-31',
    job_code INT,
    store_id INT
)
PARTITION BY RANGE ( YEAR(separated) ) (
    PARTITION p0 VALUES LESS THAN (1991),
    PARTITION p1 VALUES LESS THAN (1996),
    PARTITION p2 VALUES LESS THAN (2001),
    PARTITION p3 VALUES LESS THAN MAXVALUE
)`,
			output: `create table employees (
	id INT not null,
	fname VARCHAR(30),
	lname VARCHAR(30),
	hired DATE not null default '1970-01-01',
	separated DATE not null default '9999-12-31',
	job_code INT,
	store_id INT
)
partition by range (YEAR(separated))
(partition p0 values less than (1991),
 partition p1 values less than (1996),
 partition p2 values less than (2001),
 partition p3 values less than maxvalue)`,
		},
		{
			input: `CREATE TABLE quarterly_report_status (
    report_id INT NOT NULL,
    report_status VARCHAR(20) NOT NULL,
    report_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
PARTITION BY RANGE ( UNIX_TIMESTAMP(report_updated) ) (
    PARTITION p0 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-01-01 00:00:00') ),
    PARTITION p1 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-04-01 00:00:00') ),
    PARTITION p2 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-07-01 00:00:00') ),
    PARTITION p3 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-10-01 00:00:00') ),
    PARTITION p4 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-01-01 00:00:00') ),
    PARTITION p5 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-04-01 00:00:00') ),
    PARTITION p6 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-07-01 00:00:00') ),
    PARTITION p7 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-10-01 00:00:00') ),
    PARTITION p8 VALUES LESS THAN ( UNIX_TIMESTAMP('2010-01-01 00:00:00') ),
    PARTITION p9 VALUES LESS THAN (MAXVALUE)
)`,
			output: `create table quarterly_report_status (
	report_id INT not null,
	report_status VARCHAR(20) not null,
	report_updated TIMESTAMP not null default current_timestamp() on update current_timestamp()
)
partition by range (UNIX_TIMESTAMP(report_updated))
(partition p0 values less than (UNIX_TIMESTAMP('2008-01-01 00:00:00')),
 partition p1 values less than (UNIX_TIMESTAMP('2008-04-01 00:00:00')),
 partition p2 values less than (UNIX_TIMESTAMP('2008-07-01 00:00:00')),
 partition p3 values less than (UNIX_TIMESTAMP('2008-10-01 00:00:00')),
 partition p4 values less than (UNIX_TIMESTAMP('2009-01-01 00:00:00')),
 partition p5 values less than (UNIX_TIMESTAMP('2009-04-01 00:00:00')),
 partition p6 values less than (UNIX_TIMESTAMP('2009-07-01 00:00:00')),
 partition p7 values less than (UNIX_TIMESTAMP('2009-10-01 00:00:00')),
 partition p8 values less than (UNIX_TIMESTAMP('2010-01-01 00:00:00')),
 partition p9 values less than maxvalue)`,
		},
		{
			input: `CREATE TABLE quarterly_report_status (
    report_id INT NOT NULL,
    report_status VARCHAR(20) NOT NULL,
    report_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
PARTITION BY RANGE ( UNIX_TIMESTAMP(report_updated) ) (
    PARTITION p0 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-01-01 00:00:00') ),
    PARTITION p1 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-04-01 00:00:00') ),
    PARTITION p2 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-07-01 00:00:00') ),
    PARTITION p3 VALUES LESS THAN ( UNIX_TIMESTAMP('2008-10-01 00:00:00') ),
    PARTITION p4 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-01-01 00:00:00') ),
    PARTITION p5 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-04-01 00:00:00') ),
    PARTITION p6 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-07-01 00:00:00') ),
    PARTITION p7 VALUES LESS THAN ( UNIX_TIMESTAMP('2009-10-01 00:00:00') ),
    PARTITION p8 VALUES LESS THAN ( UNIX_TIMESTAMP('2010-01-01 00:00:00') ),
    PARTITION p9 VALUES LESS THAN (MAXVALUE)
)`,
			output: `create table quarterly_report_status (
	report_id INT not null,
	report_status VARCHAR(20) not null,
	report_updated TIMESTAMP not null default current_timestamp() on update current_timestamp()
)
partition by range (UNIX_TIMESTAMP(report_updated))
(partition p0 values less than (UNIX_TIMESTAMP('2008-01-01 00:00:00')),
 partition p1 values less than (UNIX_TIMESTAMP('2008-04-01 00:00:00')),
 partition p2 values less than (UNIX_TIMESTAMP('2008-07-01 00:00:00')),
 partition p3 values less than (UNIX_TIMESTAMP('2008-10-01 00:00:00')),
 partition p4 values less than (UNIX_TIMESTAMP('2009-01-01 00:00:00')),
 partition p5 values less than (UNIX_TIMESTAMP('2009-04-01 00:00:00')),
 partition p6 values less than (UNIX_TIMESTAMP('2009-07-01 00:00:00')),
 partition p7 values less than (UNIX_TIMESTAMP('2009-10-01 00:00:00')),
 partition p8 values less than (UNIX_TIMESTAMP('2010-01-01 00:00:00')),
 partition p9 values less than maxvalue)`,
		},
		{
			input: `CREATE TABLE members (
    firstname VARCHAR(25) NOT NULL,
    lastname VARCHAR(25) NOT NULL,
    username VARCHAR(16) NOT NULL,
    email VARCHAR(35),
    joined DATE NOT NULL
)
PARTITION BY RANGE COLUMNS(joined) (
    PARTITION p0 VALUES LESS THAN ('1960-01-01'),
    PARTITION p1 VALUES LESS THAN ('1970-01-01'),
    PARTITION p2 VALUES LESS THAN ('1980-01-01'),
    PARTITION p3 VALUES LESS THAN ('1990-01-01'),
    PARTITION p4 VALUES LESS THAN MAXVALUE
)`,
			output: `create table members (
	firstname VARCHAR(25) not null,
	lastname VARCHAR(25) not null,
	username VARCHAR(16) not null,
	email VARCHAR(35),
	joined DATE not null
)
partition by range columns (joined)
(partition p0 values less than ('1960-01-01'),
 partition p1 values less than ('1970-01-01'),
 partition p2 values less than ('1980-01-01'),
 partition p3 values less than ('1990-01-01'),
 partition p4 values less than maxvalue)`,
		},
		{
			input: `CREATE TABLE ti (id INT, amount DECIMAL(7,2), tr_date DATE)
    ENGINE=INNODB
    PARTITION BY HASH( MONTH(tr_date) )
    PARTITIONS 6`,
			output: `create table ti (
	id INT,
	amount DECIMAL(7,2),
	tr_date DATE
) ENGINE INNODB
partition by hash (MONTH(tr_date)) partitions 6`,
		},
		{
			input: `CREATE TABLE members (
    firstname VARCHAR(25) NOT NULL,
    lastname VARCHAR(25) NOT NULL,
    username VARCHAR(16) NOT NULL,
    email VARCHAR(35),
    joined DATE NOT NULL
)
PARTITION BY KEY(joined)
PARTITIONS 6`,
			output: `create table members (
	firstname VARCHAR(25) not null,
	lastname VARCHAR(25) not null,
	username VARCHAR(16) not null,
	email VARCHAR(35),
	joined DATE not null
)
partition by key (joined) partitions 6`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5),
	PARTITION MyPart VALUES IN (2,4,6)
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5),
 partition MyPart values in (2, 4, 6))`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5) STORAGE ENGINE = FOOBAR,
	PARTITION MyPart VALUES IN (2,4,6)
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5) storage engine FOOBAR,
 partition MyPart values in (2, 4, 6))`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5) ENGINE = FOOBAR,
	PARTITION MyPart VALUES IN (2,4,6)
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5) engine FOOBAR,
 partition MyPart values in (2, 4, 6))`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5) STORAGE ENGINE FOOBAR,
	PARTITION MyPart VALUES IN (2,4,6)
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5) storage engine FOOBAR,
 partition MyPart values in (2, 4, 6))`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5) STORAGE ENGINE FOOBAR COMMENT = 'test',
	PARTITION MyPart VALUES IN (2,4,6) comment 'test2'
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5) storage engine FOOBAR comment 'test',
 partition MyPart values in (2, 4, 6) comment 'test2')`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5) STORAGE ENGINE FOOBAR DATA DIRECTORY = 'test',
	PARTITION MyPart VALUES IN (2,4,6) DATA DIRECTORY 'test2'
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5) storage engine FOOBAR data directory 'test',
 partition MyPart values in (2, 4, 6) data directory 'test2')`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5) INDEX DIRECTORY = 'test',
	PARTITION MyPart VALUES IN (2,4,6) INDEX DIRECTORY 'test2'
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5) index directory 'test',
 partition MyPart values in (2, 4, 6) index directory 'test2')`,
		},
		{
			input: `create table t1 (id int primary key) partition by list (id) (partition p1 values in(11,21), partition p2 values in (12,22))`,
			output: `create table t1 (
	id int primary key
)
partition by list (id)
(partition p1 values in (11, 21),
 partition p2 values in (12, 22))`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5) COMMENT = 'before' STORAGE ENGINE FOOBAR DATA DIRECTORY = 'test',
	PARTITION MyPart VALUES IN (2,4,6) DATA DIRECTORY 'test2'
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5) storage engine FOOBAR comment 'before' data directory 'test',
 partition MyPart values in (2, 4, 6) data directory 'test2')`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5) MAX_ROWS = 4,
	PARTITION MyPart VALUES IN (2,4,6) MAX_ROWS 10
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5) max_rows 4,
 partition MyPart values in (2, 4, 6) max_rows 10)`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5) MIN_ROWS = 4,
	PARTITION MyPart VALUES IN (2,4,6) MIN_ROWS 10
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5) min_rows 4,
 partition MyPart values in (2, 4, 6) min_rows 10)`,
		},
		{
			input: `CREATE TABLE t2 (val INT)
	PARTITION BY LIST(val)(
	PARTITION mypart VALUES IN (1,3,5) TABLESPACE = innodb_system,
	PARTITION MyPart VALUES IN (2,4,6) TABLESPACE innodb_system
)`,
			output: `create table t2 (
	val INT
)
partition by list (val)
(partition mypart values in (1, 3, 5) tablespace innodb_system,
 partition MyPart values in (2, 4, 6) tablespace innodb_system)`,
		},
		{
			// index with an expression
			input: `create table t (
	id int auto_increment,
	username varchar(64),
	nickname varchar(64),
	email varchar(64),
	primary key (id),
	key email_idx (email, (if(username = '', nickname, username)))
)`,
		},
		{
			input: `create table entries (
	uid varchar(53) character set utf8mb4 collate utf8mb4_bin not null,
	namespace varchar(254) character set utf8mb4 collate utf8mb4_bin not null,
	place varchar(254) character set utf8mb4 collate utf8mb4_bin not null,
	creationTimestamp timestamp null default current_timestamp(),
	updatedTimestamp timestamp null default current_timestamp() on update current_timestamp(),
	labels json default null,
	spec json default null,
	salaryInfo json default null,
	PRIMARY KEY (namespace, uid),
	UNIQUE KEY namespaced_name (namespace, place),
	UNIQUE KEY unique_uid (uid),
	KEY entries_spec_updatedAt ((json_value(spec, _utf8mb4 '$.updatedAt')))
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_bin`,
		},
		{
			input: `CREATE TABLE t1(
    j JSON,
    INDEX i1 ( (JSON_VALUE(j, '$.id' RETURNING UNSIGNED)) )
)`,
			output: `create table t1 (
	j JSON,
	INDEX i1 ((json_value(j, '$.id' returning UNSIGNED)))
)`,
		}, {
			input: `CREATE TABLE entries (
	uid varchar(53) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
	namespace varchar(254) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
	employee varchar(254) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
	creationTimestamp timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	updatedTimestamp timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	labels json DEFAULT NULL,
	spec json DEFAULT NULL,
	salaryInfo json DEFAULT NULL,
	PRIMARY KEY (namespace,uid),
	UNIQUE KEY namespaced_employee (namespace,employee),
	UNIQUE KEY unique_uid (uid),
	KEY entries_spec_updatedAt ((json_value(spec, _utf8mb4'$.updatedAt' returning datetime)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`,
			output: `create table entries (
	uid varchar(53) character set utf8mb4 collate utf8mb4_bin not null,
	namespace varchar(254) character set utf8mb4 collate utf8mb4_bin not null,
	employee varchar(254) character set utf8mb4 collate utf8mb4_bin not null,
	creationTimestamp timestamp null default current_timestamp(),
	updatedTimestamp timestamp null default current_timestamp() on update current_timestamp(),
	labels json default null,
	spec json default null,
	salaryInfo json default null,
	PRIMARY KEY (namespace, uid),
	UNIQUE KEY namespaced_employee (namespace, employee),
	UNIQUE KEY unique_uid (uid),
	KEY entries_spec_updatedAt ((json_value(spec, _utf8mb4 '$.updatedAt' returning datetime)))
) ENGINE InnoDB,
  CHARSET utf8mb4,
  COLLATE utf8mb4_bin`,
		}, {
			// Subpartitions
			input: `CREATE TABLE ts (id INT, purchased DATE)
    PARTITION BY RANGE( YEAR(purchased) )
    SUBPARTITION BY HASH( TO_DAYS(purchased) ) (
        PARTITION p0 VALUES LESS THAN (1990) (
            SUBPARTITION s0,
            SUBPARTITION s1
        ),
        PARTITION p1 VALUES LESS THAN (2000) (
            SUBPARTITION s2,
            SUBPARTITION s31
        ),
        PARTITION p2 VALUES LESS THAN MAXVALUE (
            SUBPARTITION s4,
            SUBPARTITION s5
        )
    )`,
			output: `create table ts (
	id INT,
	purchased DATE
)
partition by range (YEAR(purchased)) subpartition by hash (TO_DAYS(purchased))
(partition p0 values less than (1990) (subpartition s0, subpartition s1),
 partition p1 values less than (2000) (subpartition s2, subpartition s31),
 partition p2 values less than maxvalue (subpartition s4, subpartition s5))`,
		},
		{
			input: `CREATE TABLE ts (id INT, purchased DATE)
    PARTITION BY RANGE( YEAR(purchased) )
    SUBPARTITION BY HASH( TO_DAYS(purchased) ) (
        PARTITION p0 VALUES LESS THAN (1990) (
            SUBPARTITION s0 STORAGE Engine = 'innodb' data directory = '/data',
            SUBPARTITION s1 COMMENT = 'this is s1' index directory = '/index'
        ),
        PARTITION p1 VALUES LESS THAN (2000) (
            SUBPARTITION s2 MAx_rows = 4,
            SUBPARTITION s31 min_rows = 5 tablespace = t2
        ),
        PARTITION p2 VALUES LESS THAN MAXVALUE (
            SUBPARTITION s4,
            SUBPARTITION s5
        )
    )`,
			output: `create table ts (
	id INT,
	purchased DATE
)
partition by range (YEAR(purchased)) subpartition by hash (TO_DAYS(purchased))
(partition p0 values less than (1990) (subpartition s0 storage engine innodb data directory '/data', subpartition s1 comment 'this is s1' index directory '/index'),
 partition p1 values less than (2000) (subpartition s2 max_rows 4, subpartition s31 min_rows 5 tablespace t2),
 partition p2 values less than maxvalue (subpartition s4, subpartition s5))`,
		},
		{
			input:  "create table t (i bigint) charset ascii",
			output: "create table t (\n\ti bigint\n) charset ascii",
		},
		{
			input:  "create table t (i1 char ascii, i2 char character set ascii, i3 char binary, i4 char unicode binary, i5 char binary unicode, i6 char ascii binary, i7 char binary ascii, i8 char byte, i9 char character set latin1 binary)",
			output: "create table t (\n\ti1 char character set latin1,\n\ti2 char character set ascii,\n\ti3 char binary,\n\ti4 char character set ucs2 binary,\n\ti5 char character set ucs2 binary,\n\ti6 char character set latin1 binary,\n\ti7 char character set latin1 binary,\n\ti8 binary,\n\ti9 char character set latin1 binary\n)",
		},
		{
			input:  "create table t (id int, info JSON, INDEX zips((CAST(info->'$.field' AS unsigned ARRAY))))",
			output: "create table t (\n\tid int,\n\tinfo JSON,\n\tINDEX zips ((cast(info -> '$.field' as unsigned array)))\n)",
		},
	}
	for _, test := range createTableQueries {
		sql := strings.TrimSpace(test.input)
		t.Run(sql, func(t *testing.T) {
			tree, err := ParseStrictDDL(sql)
			require.NoError(t, err)
			got := String(tree)
			expected := test.output
			if expected == "" {
				expected = sql
			}
			require.Equal(t, expected, got)
		})
	}
}

func TestOne(t *testing.T) {
	testOne := struct {
		input, output string
	}{
		input:  "",
		output: "",
	}
	if testOne.input == "" {
		return
	}
	sql := strings.TrimSpace(testOne.input)
	tree, err := Parse(sql)
	require.NoError(t, err)
	got := String(tree)
	expected := testOne.output
	if expected == "" {
		expected = sql
	}
	require.Equal(t, expected, got)
}

func TestCreateTableLike(t *testing.T) {
	normal := "create table a like b"
	testCases := []struct {
		input  string
		output string
	}{
		{
			"create table a like b",
			normal,
		},
		{
			"create table a (like b)",
			normal,
		},
		{
			"create table ks.a like unsharded_ks.b",
			"create table ks.a like unsharded_ks.b",
		},
	}
	for _, tcase := range testCases {
		tree, err := ParseStrictDDL(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		assert.True(t, tree.(*CreateTable).FullyParsed)
		if got, want := String(tree.(*CreateTable)), tcase.output; got != want {
			t.Errorf("Parse(%s):\n%s, want\n%s", tcase.input, got, want)
		}
	}
}

func TestCreateTableEscaped(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{{
		input: "create table `a`(`id` int, primary key(`id`))",
		output: "create table a (\n" +
			"\tid int,\n" +
			"\tprimary key (id)\n" +
			")",
	}, {
		input: "create table `insert`(`update` int, primary key(`delete`))",
		output: "create table `insert` (\n" +
			"\t`update` int,\n" +
			"\tprimary key (`delete`)\n" +
			")",
	}}
	for _, tcase := range testCases {
		tree, err := ParseStrictDDL(tcase.input)
		if err != nil {
			t.Errorf("input: %s, err: %v", tcase.input, err)
			continue
		}
		if got, want := String(tree.(*CreateTable)), tcase.output; got != want {
			t.Errorf("Parse(%s):\n%s, want\n%s", tcase.input, got, want)
		}
	}
}

var (
	invalidSQL = []struct {
		input        string
		output       string
		excludeMulti bool // Don't use in the ParseNext multi-statement parsing tests.
	}{{
		input:  "select : from t",
		output: "syntax error at position 9 near ':'",
	}, {
		input:  "execute stmt using 1;",
		output: "syntax error at position 21 near '1'",
	}, {
		input:  "PREPARE stmt FROM a;",
		output: "syntax error at position 20 near 'a'",
	}, {
		input:  "PREPARE stmt FROM @@a;",
		output: "syntax error at position 22 near 'a'",
	}, {
		input:  "select x'78 from t",
		output: "syntax error at position 12 near '78'",
	}, {
		input:  "select x'777' from t",
		output: "syntax error at position 14 near '777'",
	}, {
		input:  "select * from t where :1f = 2",
		output: "syntax error at position 26 near 'f'",
	}, {
		input:  "select * from t where :. = 2",
		output: "syntax error at position 24 near ':'",
	}, {
		input:  "select * from t where ::1 = 2",
		output: "syntax error at position 25 near '::'",
	}, {
		input:  "select * from t where ::. = 2",
		output: "syntax error at position 25 near '::'",
	}, {
		input:  "update a set c = values(1)",
		output: "syntax error at position 26 near '1'",
	}, {
		input: "select(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(" +
			"F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F(F" +
			"(F(F(F(F(F(F(F(F(F(F(F(F(",
		output: "syntax error at position 406",
	}, {
		// This construct is considered invalid due to a grammar conflict.
		input:  "insert into a select * from b join c on duplicate key update d=e",
		output: "syntax error at position 54 near 'key'",
	}, {
		input:  "select * from a left join b",
		output: "syntax error at position 28",
	}, {
		input:  "select * from a natural join b on c = d",
		output: "syntax error at position 34 near 'on'",
	}, {
		input:  "select * from a natural join b using (c)",
		output: "syntax error at position 37 near 'using'",
	}, {
		input:  "select next id from a",
		output: "expecting value after next at position 15 near 'id'",
	}, {
		input:  "select next 1+1 values from a",
		output: "syntax error at position 15",
	}, {
		input:  "insert into a values (select * from b)",
		output: "syntax error at position 29 near 'select'",
	}, {
		input:  "select database",
		output: "syntax error at position 16",
	}, {
		input:  "select mod from t",
		output: "syntax error at position 16 near 'from'",
	}, {
		input:  "select 1 from t where div 5",
		output: "syntax error at position 26 near 'div'",
	}, {
		input:  "select 1 from t where binary",
		output: "syntax error at position 29",
	}, {
		input:  "select match(a1, a2) against ('foo' in boolean mode with query expansion) from t",
		output: "syntax error at position 57 near 'with'",
	}, {
		input:  "select /* reserved keyword as unqualified column */ * from t where key = 'test'",
		output: "syntax error at position 71 near 'key'",
	}, {
		input:  "select /* vitess-reserved keyword as unqualified column */ * from t where escape = 'test'",
		output: "syntax error at position 81 near 'escape'",
	}, {
		input:  "select /* straight_join using */ 1 from t1 straight_join t2 using (a)",
		output: "syntax error at position 66 near 'using'",
	}, {
		input:        "select 'aa",
		output:       "syntax error at position 11 near 'aa'",
		excludeMulti: true,
	}, {
		input:        "select 'aa\\",
		output:       "syntax error at position 12 near 'aa'",
		excludeMulti: true,
	}, {
		input:        "select /* aa",
		output:       "syntax error at position 13 near '/* aa'",
		excludeMulti: true,
	}, {
		// This is a valid MySQL query but does not yet work with Vitess.
		// The problem is that the tokenizer takes .3 as a single token which causes parsing error
		// We should instead be using . as a separate token and then 3t2 as an identifier.
		// This highlights another problem, the tokenization has to be aware of the context of parsing!
		// Since in an alternate query like `select .3e3t`, we should use .3e3 as a single token FLOAT and then t as ID.
		input:        "create table 2t.3t2 (c1 bigint not null, c2 text, primary key(c1))",
		output:       "syntax error at position 18 near '.3'",
		excludeMulti: true,
	}, {
		input:  "execute stmt1 using a, @b",
		output: "syntax error at position 22 near 'a'",
	}, {
		input:  "create index @a on b (col1)",
		output: "syntax error at position 16 near 'a'",
	}, {
		input:  "create database test_db default collate @a",
		output: "syntax error at position 43 near 'a'",
	}, {
		input:  "create database test_db default charset @a",
		output: "syntax error at position 43 near 'a'",
	}, {
		input:  "create database test_db default encryption @a",
		output: "syntax error at position 46 near 'a'",
	}}
)

func TestErrors(t *testing.T) {
	for _, tcase := range invalidSQL {
		t.Run(tcase.input, func(t *testing.T) {
			_, err := ParseStrictDDL(tcase.input)
			require.Error(t, err, tcase.output)
			require.Equal(t, tcase.output, err.Error())
		})
	}
}

// TestSkipToEnd tests that the skip to end functionality
// does not skip past a ';'. If any tokens exist after that, Parse
// should return an error.
func TestSkipToEnd(t *testing.T) {
	testcases := []struct {
		input  string
		output string
	}{{
		// This is the case where the partial ddl will be reset
		// because of a premature ';'.
		input:  "create table a(id; select * from t",
		output: "syntax error at position 19",
	}, {
		// Partial DDL should get reset for valid DDLs also.
		input:  "create table a(id int); select * from t",
		output: "syntax error at position 31 near 'select'",
	}, {
		// Partial DDL does not get reset here. But we allow the
		// DDL only if there are no new tokens after skipping to end.
		input:  "create table a bb cc; select * from t",
		output: "extra characters encountered after end of DDL: 'select'",
	}, {
		// Test that we don't step at ';' inside strings.
		input:  "create table a bb 'a;'; select * from t",
		output: "extra characters encountered after end of DDL: 'select'",
	}}
	for _, tcase := range testcases {
		_, err := Parse(tcase.input)
		if err == nil || err.Error() != tcase.output {
			t.Errorf("%s: %v, want %s", tcase.input, err, tcase.output)
		}
	}
}

func loadQueries(t testing.TB, filename string) (queries []string) {
	file, err := os.Open(path.Join("testdata", filename))
	require.NoError(t, err)
	defer file.Close()

	var read io.Reader
	if strings.HasSuffix(filename, ".gz") {
		gzread, err := gzip.NewReader(file)
		if err != nil {
			t.Fatal(err)
		}
		defer gzread.Close()
		read = gzread
	} else {
		read = file
	}

	scanner := bufio.NewScanner(read)
	for scanner.Scan() {
		queries = append(queries, scanner.Text())
	}
	return queries
}

func TestParseDjangoQueries(t *testing.T) {
	for _, query := range loadQueries(t, "django_queries.txt") {
		_, err := Parse(query)
		if err != nil {
			t.Errorf("failed to parse %q: %v", query, err)
		}
	}
}

func TestParseLobstersQueries(t *testing.T) {
	for _, query := range loadQueries(t, "lobsters.sql.gz") {
		_, err := Parse(query)
		if err != nil {
			t.Errorf("failed to parse %q: %v", query, err)
		}
	}
}

func TestParseVersionedComments(t *testing.T) {
	testcases := []struct {
		input        string
		mysqlVersion string
		output       string
	}{
		{
			input:        `CREATE TABLE table1 (id int) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!50900 PARTITION BY RANGE (id) (PARTITION x VALUES LESS THAN (5) ENGINE = InnoDB, PARTITION t VALUES LESS THAN (20) ENGINE = InnoDB) */`,
			mysqlVersion: "50401",
			output: `create table table1 (
	id int
) ENGINE InnoDB,
  CHARSET utf8mb4`,
		}, {
			input:        `CREATE TABLE table1 (id int) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!50900 PARTITION BY RANGE (id) (PARTITION x VALUES LESS THAN (5) ENGINE = InnoDB, PARTITION t VALUES LESS THAN (20) ENGINE = InnoDB) */`,
			mysqlVersion: "80001",
			output: `create table table1 (
	id int
) ENGINE InnoDB,
  CHARSET utf8mb4
partition by range (id)
(partition x values less than (5) engine InnoDB,
 partition t values less than (20) engine InnoDB)`,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.input+":"+testcase.mysqlVersion, func(t *testing.T) {
			oldMySQLVersion := mySQLParserVersion
			defer func() { mySQLParserVersion = oldMySQLVersion }()
			mySQLParserVersion = testcase.mysqlVersion
			tree, err := Parse(testcase.input)
			require.NoError(t, err, testcase.input)
			out := String(tree)
			require.Equal(t, testcase.output, out)
		})
	}
}

func BenchmarkParseTraces(b *testing.B) {
	for _, trace := range []string{"django_queries.txt", "lobsters.sql.gz"} {
		b.Run(trace, func(b *testing.B) {
			queries := loadQueries(b, trace)
			if len(queries) > 10000 {
				queries = queries[:10000]
			}
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for _, query := range queries {
					_, err := Parse(query)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}

}

func BenchmarkParseStress(b *testing.B) {
	const (
		sql1 = "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
		sql2 = "select aaaa, bbb, ccc, ddd, eeee, ffff, gggg, hhhh, iiii from tttt, ttt1, ttt3 where aaaa = bbbb and bbbb = cccc and dddd+1 = eeee group by fff, gggg having hhhh = iiii and iiii = jjjj order by kkkk, llll limit 3, 4"
	)

	for i, sql := range []string{sql1, sql2} {
		b.Run(fmt.Sprintf("sql%d", i), func(b *testing.B) {
			var buf bytes.Buffer
			buf.WriteString(sql)
			querySQL := buf.String()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := Parse(querySQL)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkParse3(b *testing.B) {
	largeQueryBenchmark := func(b *testing.B, escape bool) {
		b.Helper()

		// benchQuerySize is the approximate size of the query.
		benchQuerySize := 1000000

		// Size of value is 1/10 size of query. Then we add
		// 10 such values to the where clause.
		var baseval bytes.Buffer
		for i := 0; i < benchQuerySize/100; i++ {
			// Add an escape character: This will force the upcoming
			// tokenizer improvement to still create a copy of the string.
			// Then we can see if avoiding the copy will be worth it.
			if escape {
				baseval.WriteString("\\'123456789")
			} else {
				baseval.WriteString("123456789")
			}
		}

		var buf bytes.Buffer
		buf.WriteString("select a from t1 where v = 1")
		for i := 0; i < 10; i++ {
			fmt.Fprintf(&buf, " and v%d = \"%d%s\"", i, i, baseval.String())
		}
		benchQuery := buf.String()
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if _, err := Parse(benchQuery); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("normal", func(b *testing.B) {
		largeQueryBenchmark(b, false)
	})

	b.Run("escaped", func(b *testing.B) {
		largeQueryBenchmark(b, true)
	})
}

func TestValidUnionCases(t *testing.T) {
	testFile(t, "union_cases.txt", makeTestOutput(t))
}

func TestValidSelectCases(t *testing.T) {
	testFile(t, "select_cases.txt", makeTestOutput(t))
}

func makeTestOutput(t *testing.T) string {
	testOutputTempDir := utils.MakeTestOutput(t, "testdata", "parse_test")

	t.Cleanup(func() {
		if !t.Failed() {
			_ = os.RemoveAll(testOutputTempDir)
		} else {
			t.Logf("Errors found. If the output is correct, run `cp %s/* testdata/` to update test expectations", testOutputTempDir)
		}
	})

	return testOutputTempDir
}

type testCase struct {
	file     string
	lineno   int
	input    string
	output   string
	errStr   string
	comments string
}

func escapeNewLines(in string) string {
	return strings.ReplaceAll(in, "\n", "\\n")
}

func testFile(t *testing.T, filename, tempDir string) {
	t.Run(filename, func(t *testing.T) {
		fail := false
		expected := strings.Builder{}
		for tcase := range iterateExecFile(filename) {
			t.Run(fmt.Sprintf("%d : %s", tcase.lineno, tcase.input), func(t *testing.T) {
				if tcase.output == "" && tcase.errStr == "" {
					tcase.output = tcase.input
				}
				expected.WriteString(fmt.Sprintf("%sINPUT\n%s\nEND\n", tcase.comments, escapeNewLines(tcase.input)))
				tree, err := Parse(tcase.input)
				if tcase.errStr != "" {
					errPresent := ""
					if err != nil {
						errPresent = err.Error()
					}
					expected.WriteString(fmt.Sprintf("ERROR\n%s\nEND\n", escapeNewLines(errPresent)))
					if err == nil || tcase.errStr != err.Error() {
						fail = true
						t.Errorf("File: %s, Line: %d\nDiff:\n%s\n[%s] \n[%s]", filename, tcase.lineno, cmp.Diff(tcase.errStr, errPresent), tcase.errStr, errPresent)
					}
				} else {
					if err != nil {
						expected.WriteString(fmt.Sprintf("ERROR\n%s\nEND\n", escapeNewLines(err.Error())))
						fail = true
						t.Errorf("File: %s:%d\nDiff:\n%s\n[%s] \n[%s]", filename, tcase.lineno, cmp.Diff(tcase.errStr, err.Error()), tcase.errStr, err.Error())
					} else {
						out := String(tree)
						expected.WriteString(fmt.Sprintf("OUTPUT\n%s\nEND\n", escapeNewLines(out)))
						if tcase.output != out {
							fail = true
							t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", tcase.output, out)
						}
					}
				}
			})
		}

		if fail && tempDir != "" {
			gotFile := fmt.Sprintf("%s/%s", tempDir, filename)
			_ = os.WriteFile(gotFile, []byte(strings.TrimSpace(expected.String())+"\n"), 0644)
			fmt.Println(fmt.Sprintf("Errors found in parse tests. If the output is correct, run `cp %s/* testdata/` to update test expectations", tempDir)) // nolint
		}
	})
}

func iterateExecFile(name string) (testCaseIterator chan testCase) {
	name = locateFile(name)
	fd, err := os.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		panic(fmt.Sprintf("Could not open file %s", name))
	}

	testCaseIterator = make(chan testCase)
	var comments string
	go func() {
		defer close(testCaseIterator)

		r := bufio.NewReader(fd)
		lineno := 0
		var output string
		var returnTypeNumber int
		var input string
		for {
			input, lineno, _ = parsePartial(r, []string{"INPUT"}, lineno, name)
			if input == "" && lineno == 0 {
				break
			}
			output, lineno, returnTypeNumber = parsePartial(r, []string{"OUTPUT", "ERROR"}, lineno, name)
			var errStr string
			if returnTypeNumber == 1 {
				errStr = output
				output = ""
			}
			testCaseIterator <- testCase{
				file:     name,
				lineno:   lineno,
				input:    input,
				comments: comments,
				output:   output,
				errStr:   errStr,
			}
			comments = ""
		}
	}()
	return testCaseIterator
}

func parsePartial(r *bufio.Reader, readType []string, lineno int, fileName string) (string, int, int) {
	returnTypeNumber := -1
	for {
		binput, err := r.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				panic(fmt.Errorf("error reading file %s: line %d: %s", fileName, lineno, err.Error()))
			}
			return "", 0, 0
		}
		lineno++
		input := string(binput)
		input = strings.TrimSpace(input)
		if input == "" || input == "\n" {
			continue
		}
		for i, str := range readType {
			if input == str {
				returnTypeNumber = i
				break
			}
		}
		if returnTypeNumber != -1 {
			break
		}
		panic(fmt.Errorf("error reading file %s: line %d: %s - Expected keyword", fileName, lineno, err.Error()))
	}
	input := ""
	for {
		l, err := r.ReadBytes('\n')
		lineno++
		if err != nil {
			panic(fmt.Sprintf("error reading file %s line# %d: %s", fileName, lineno, err.Error()))
		}
		str := strings.TrimSpace(string(l))
		if str == "END" {
			break
		}
		if input == "" {
			input += str
		} else {
			input += str + "\n"
		}
	}
	return input, lineno, returnTypeNumber
}

func locateFile(name string) string {
	return "testdata/" + name
}
