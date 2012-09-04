/* Copyright 2012, Google Inc. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can
 * be found in the LICENSE file.
 */

/* Fast BSON decoding for Python */

/* Sample code (uses bson module for encoding):

  >>> s = bson.dumps({'world': 'hello'})
  >>> cbson.loads(s)
  {'world': 'hello'}

  >>> s = bson.dumps({'a': 1}) + bson.dumps({'b': 2.0}) + bson.dumps({'c': [None]})
  >>> cbson.decode_next(s)
  (12, {'a': 1})     # 12 is the offset into s
  >>> cbson.decode_next(s, 12)
  (28, {'b': 2.0})
  >>> cbson.decode_next(s, 28)
  (44, {'c': [None]})
*/

/*#include <string.h>*/
#include "Python.h"

#if PY_VERSION_HEX < 0x02020000
#error Requires Python 2.2 or newer.
#endif

#if PY_VERSION_HEX < 0x02050000
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif

static PyObject *BSONError;
static PyObject *BSONBufferTooShort;

/* -------------------------- BufIter methods ------------------------------ */

/* Slice is a segment of the buffer we are currently looking at. After
 * initialization, slice.start is same as buffer->start and size is
 * 0. next() updates slice so it points to a segment immediately
 * following the current slice. */
typedef struct _Slice {
  const char* start;
  uint32_t size;
} Slice;

/* BufIter represents the buffer object holding BSON-encoded bytes
   Only access via next*(), too_short(), VAL_AT, PTR_AT, INDEX_OF.
 */
typedef struct _BufIter {
  const char* start; /* first byte of buffer */
  const char* end;   /* last byte of buffer */
  Slice slice; /* what we are currently looking at */
} BufIter;

/* current position within buffer */
#define INDEX_OF(buf_iter) (int)(buf_iter->slice.start - buf_iter->start)

/* typecasted pointer to start of current position */
#define PTR_AT(buf_iter, typecast) (typecast)(buf_iter->slice.start)

/* typecasted value at current position */
#define VAL_AT(buf_iter, typecast) *(PTR_AT(buf_iter, typecast*))

/* too_short() returns 0 if at least remaining bytes exist beyond the
   end of the current slice. Else it returns the number of bytes
   short. */
static inline uint32_t too_short(BufIter* buf_iter, uint32_t remaining) {
  const char* last;
  last = buf_iter->slice.start + buf_iter->slice.size + remaining - 1;
  if ((last > buf_iter->end) || (last < buf_iter->slice.start - 1))
    return last - buf_iter->end;
  else
    return 0;
}

/* next() moves buf_iter's slice forward so it spans the next inc
   bytes. (See Slice above.) Returns 1 on success. If inc bytes are
   not available after end of current slice then it sets exception and
   returns 0. tag is for better error reporting. */
static inline int next(BufIter* buf_iter, uint32_t inc, const char* tag) {
  uint32_t position;
  if (too_short(buf_iter, inc)) {
    position = buf_iter->slice.start - buf_iter->start + buf_iter->slice.size;
    PyErr_Format(BSONError,
                 "unexpected end of buffer: wanted %u bytes at buffer[%u] for %s",
                 inc, position, tag);
    return 0;
    }

  buf_iter->slice.start = buf_iter->slice.start + buf_iter->slice.size;
  buf_iter->slice.size = inc;
  return 1;
}

/* next_cstring() is similar to next() - but it moves the slice
   forward to span the next NULL terminated string. Returns 1 on
   success, 0 if no NULL found before end of buffer. */
static inline int next_cstring(BufIter* buf_iter, const char* tag) {
  const char* position;
  next(buf_iter, 0, tag);
  position = buf_iter->slice.start;
  while (position <= buf_iter->end) {
    if (*position==0) {
      break;
    }
    position++;
  }
  if (position > buf_iter->end) {
    PyErr_Format(BSONError,
                 "unexpected end of buffer: non-terminated cstring at buffer[%u] for %s",
                 (unsigned int)(buf_iter->slice.start - buf_iter->start), tag);
    return 0;
  }

  buf_iter->slice.size = (position - buf_iter->slice.start + 1); // +1 adjusts for trailing '\x00'
  return 1;
}

/* Initialize a BufIter and a Py_buffer from a Python buffer object */
static int buf_iter_from_buffer(PyObject* buffer_obj, BufIter* buf_iter, Py_buffer* pbuffer) {
 if (!PyObject_CheckBuffer(buffer_obj)) {
   PyErr_SetString(BSONError, "argument must support buffer api");
   return 0;
 }

 if (PyObject_GetBuffer(buffer_obj, pbuffer, PyBUF_SIMPLE) < 0)
   return 0;

 if (pbuffer->len == 0) {
   PyBuffer_Release(pbuffer);
   PyErr_SetString(BSONError, "empty buffer");
   return 0;
 }

 buf_iter->start = (char*)(pbuffer->buf);
 buf_iter->end = buf_iter->start + (pbuffer->len) - 1;

 buf_iter->slice.start = buf_iter->start;
 buf_iter->slice.size = 0;

 return 1;
}

/* ------------------------------------------------------------------------ */


/* decode_element is the function prototype for parsing differnt
 * values in the bson stream */
typedef PyObject* (*decode_element)(BufIter* buf_iter);

/* list of some decoders, indexed by type_id */
static decode_element decoders[];

/* IMPORTANT: ElementTypes, sizeof(element_types) and
 * element_type_names *must* be kept in sync */
enum ElementTypes {
  binary_generic,
  binary_function,
  binary_old,
  binary_uuid,
  binary_md5,
  binary_user_defined,
  object_id,
  utc,
  regex,
  db_pointer,
  js,
  symbol,
  scoped_js,
  timestamp,
  min,
  max,
};

static char* element_type_names[] = {
  "binary_generic",
  "binary_function",
  "binary_old",
  "binary_uuid",
  "binary_md5",
  "binary_user_defined",
  "object_id",
  "utc",
  "regex",
  "db_pointer",
  "js",
  "symbol",
  "scoped_js",
  "timestamp",
  "min",
  "max",
};

static PyObject* element_types[16] = {
  NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL,
};


/* ----------------------------------- decoders ----------------------------------- */

static PyObject* decode_document(BufIter* buf_iter);
static PyObject* decode_uint64(BufIter* buf_iter);

static PyObject*
decode_next(PyObject *self, PyObject* args) {
 PyObject* buffer_obj;
 Py_buffer buffer;
 BufIter mbuf_iter;
 int offset;
 PyObject* decoded_obj;

 offset = 0;

 if (!PyArg_ParseTuple(args, "O|i:decode_next", &buffer_obj, &offset))
   return NULL;

 if (!buf_iter_from_buffer(buffer_obj, &mbuf_iter, &buffer))
     return NULL;

 if (offset < 0) {
   PyErr_Format(BSONError, "invalid negative offset %i", offset);
   PyBuffer_Release(&buffer);
   return 0;
 }

 /* jump over offset bytes */
 if (offset)
   if (!next(&mbuf_iter, offset, "specified offset")) {
     PyBuffer_Release(&buffer);
     return NULL;
   }

 decoded_obj = decode_document(&mbuf_iter);
 if (!decoded_obj) {
   PyBuffer_Release(&buffer);
   return NULL;
 }

 /* move slice to end of current slice for easy offset calculation */
 next(&mbuf_iter, 0, "end");
 offset = (mbuf_iter.slice.start - mbuf_iter.start);

 PyBuffer_Release(&buffer);
 return Py_BuildValue("iN", offset, decoded_obj);
}

static PyObject*
loads(PyObject *self, PyObject* args) {
 PyObject* buffer_obj;
 PyObject* result;
 Py_buffer buffer;
 BufIter mbuf_iter;

 if (!PyArg_ParseTuple(args, "O:load", &buffer_obj))
   return NULL;

 if (!buf_iter_from_buffer(buffer_obj, &mbuf_iter, &buffer))
     return NULL;

 result = decode_document(&mbuf_iter);
 PyBuffer_Release(&buffer);
 return result;
}

static inline int scan_int32(BufIter* buf_iter, uint32_t* result,
                             const char* tag) {
  if (!next(buf_iter, 4, tag)) return 0;
  *result = VAL_AT(buf_iter, uint32_t);
  return 1;
}

static inline int scan_int64(BufIter* buf_iter, uint64_t* result,
                             const char* tag) {
  if (!next(buf_iter, 4, tag)) return 0;
  *result = VAL_AT(buf_iter, uint64_t);
  return 1;
}

static inline PyObject*
_decode_document(BufIter* buf_iter, int is_array) {
  uint32_t doc_size, element_idx;
  PyObject *doc_obj;
  unsigned char type_id;

  PyObject* element_name;
  PyObject* element_value;

  decode_element decoder_func;

  uint32_t bytes_short;
  PyObject* error_obj;

  doc_obj = NULL;
  element_name = NULL;
  element_value = NULL;

  if (!scan_int32(buf_iter, &doc_size, "document-length"))
    return NULL;

  bytes_short = too_short(buf_iter, doc_size-4);
  if (bytes_short) {
    error_obj = Py_BuildValue(
        "Nk",
        PyString_FromFormat("buffer too short: "                        \
                            "buffer[%d:] does not contain %d bytes for document",
                            INDEX_OF(buf_iter), doc_size-4),
        bytes_short);
    if (!error_obj)
      return NULL;
    PyErr_SetObject(BSONBufferTooShort, error_obj);
    return NULL;
  }

  if (is_array)
    doc_obj = PyList_New(0);
  else
    doc_obj = PyDict_New();

  if (!doc_obj) return NULL;

  if (doc_size < 5) {
    /* This is invalid because doc_size includes the int32 size
     * (i.e. itself) and the trailing \x00 */
    PyErr_Format(BSONError, "invalid document size: %u", doc_size);
    goto error;
  }

  element_idx = 0;
  while (1) {
    if (!next(buf_iter, 1, "tag-id"))
      goto error;

    type_id = VAL_AT(buf_iter, char);

    if (type_id == 0)
      /* end of document marker */
      return doc_obj;

    if (!next_cstring(buf_iter, "element-name"))
      goto error;

    if (!is_array) {
      element_name = PyString_FromString(PTR_AT(buf_iter, const char*));
      if (!element_name) {
        goto error;
      }
    }

    /* type_ids from 0x01 thru 0x12 */
    if (type_id > 0 && type_id <= 0x12) {
      decoder_func = decoders[(int)type_id];
      element_value = decoder_func(buf_iter);
    }
    /* uint64 special case */
    else if (type_id == 0x3f) {
      element_value = decode_uint64(buf_iter);
    }
    /* two special cases of type_id > 0x12 - for min and max */
    else if (type_id == 0x7f) {
      element_value = PyTuple_Pack(1, element_types[min]);
    }
    else if (type_id == 0xff) {
      element_value = PyTuple_Pack(1, element_types[max]);
    }
    else {
      PyErr_Format(BSONError,
                   "invalid element type id 0x%x at buffer[%d] for %s",
                   type_id, INDEX_OF(buf_iter),
                   PyString_AsString(element_name));
      goto error;
    }

    if (!element_value) {
      goto error;
    }

    /* we use dict for documents, list for arrays */
    if (is_array) {
      if (PyList_Append(doc_obj, element_value) < 0) {
        goto error;
      }
      else {
        Py_CLEAR(element_value);
      }
    }
    else {
      if (PyDict_SetItem(doc_obj, element_name, element_value) < 0) {
        goto error;
      }
      else {
        Py_CLEAR(element_name);
        Py_CLEAR(element_value);
      }
    }

    element_idx += 1;
  }

error:
  Py_XDECREF(doc_obj);
  Py_XDECREF(element_name);
  Py_XDECREF(element_value);
  return NULL;
}

static PyObject* decode_document(BufIter* buf_iter){
  return _decode_document(buf_iter, 0);
}

static PyObject* decode_array(BufIter* buf_iter) {
  return _decode_document(buf_iter, 1);
}

static PyObject* decode_binary(BufIter* buf_iter) {
  uint32_t binary_size;
  unsigned char subtype;
  PyObject* eltype;
  PyObject* binary_buf;
  PyObject* result;
  eltype = NULL;
  binary_buf = NULL;
  if (!scan_int32(buf_iter, &binary_size, "binary-size"))
    return NULL;
  if (!next(buf_iter, 1, "binary-subtype"))
    return NULL;

  subtype = VAL_AT(buf_iter, unsigned char);
  switch (subtype) {
      case 0x00:
        eltype = element_types[binary_generic];
        break;
      case 0x01:
        eltype = element_types[binary_function];
        break;
      case 0x02:
        eltype = element_types[binary_old];
        break;
      case 0x03:
        eltype = element_types[binary_uuid];
        break;
      case 0x05:
        eltype = element_types[binary_md5];
        break;
      case 0x80:
        eltype = element_types[binary_user_defined];
        break;
    default:
      PyErr_Format(BSONError,
                   "invalid binary subtype 0x%x at buffer[%d]",
                   subtype, INDEX_OF(buf_iter));
      goto error;
    }

  if (!next(buf_iter, binary_size, "binary-buffer"))
    return NULL;

  binary_buf = PyString_FromStringAndSize(PTR_AT(buf_iter, const char*), binary_size);
  if (!binary_buf)
    return NULL;

  /* special case - we just return a normal Python string nfor
   * binary_generic elements - this imitates the other bson module's
   * behavior */
  if (eltype == element_types[binary_generic]) {
    return binary_buf;
  }
  else {
    /* a tuple such as ('binary_md5', '<buffer content>') */
    result = PyTuple_Pack(2, eltype, binary_buf);
  }

  if (!result) goto error;

  Py_DECREF(binary_buf);
  return result;

error:
    Py_DECREF(binary_buf);
    return NULL;
}

static inline PyObject* decode_string(BufIter* buf_iter) {
  PyObject* result;
  uint32_t elem_size;

  if (!scan_int32(buf_iter, &elem_size, "string-length"))
    return 0;
  if (!next_cstring(buf_iter, "string-body")) return 0;

  result = PyUnicode_FromStringAndSize(PTR_AT(buf_iter, const char*), elem_size-1);
  if (!result)
    return NULL;

  return result;
}

static PyObject* decode_bool(BufIter* buf_iter) {
  PyObject *result;
  if (!next(buf_iter, 1, "bool-value"))
    return NULL;

  if (VAL_AT(buf_iter, char) == 0) {
    result = Py_False;
  }
  else {
    result = Py_True;
  }

  Py_INCREF(result);
  return result;
}

static PyObject* decode_object_id(BufIter* buf_iter){
  PyObject* oid;
  PyObject* result;
  if (!next(buf_iter, 12, "object-id"))
    return NULL;
  oid = PyString_FromStringAndSize(PTR_AT(buf_iter, const char*), 12);
  if (!oid) return NULL;

  result = PyTuple_Pack(2, element_types[object_id], oid);
  if (!result) goto error;

  Py_DECREF(oid);
  return result;

error:
  Py_DECREF(oid);
  return NULL;
}

static PyObject* decode_null(BufIter* buf_iter){
  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject* decode_regex(BufIter* buf_iter){
  PyObject* regex_flags;
  PyObject* regex_str;
  PyObject* result;
  regex_flags = NULL;
  regex_str = NULL;

  if (!next_cstring(buf_iter, "regex-flags")) return NULL;
  regex_flags = PyString_FromString(PTR_AT(buf_iter, const char*));
  if (!regex_flags) goto error;

  if (!next_cstring(buf_iter, "regex-str")) return NULL;
  regex_str = PyString_FromString(PTR_AT(buf_iter, const char*));
  if (!regex_str) goto error;

  result = PyTuple_Pack(3, element_types[regex],
                        regex_flags, regex_str);

  if (!result) goto error;

  Py_DECREF(regex_flags);
  Py_DECREF(regex_str);
  return result;

error:
  Py_XDECREF(regex_flags);
  Py_XDECREF(regex_str);
  return NULL;
}

static PyObject* decode_js(BufIter* buf_iter){
  PyObject* jscode;
  PyObject* result;

  jscode = decode_string(buf_iter);
  if (!jscode) return NULL;

  result = PyTuple_Pack(2, element_types[js], jscode);
  if (!result) goto error;

  Py_DECREF(jscode);
  return result;

error:
  Py_DECREF(jscode);
  return NULL;
}

static PyObject* decode_symbol(BufIter* buf_iter){
  PyObject* sval;
  PyObject* result;

  sval = decode_string(buf_iter);
  if (!sval) return NULL;

  result = PyTuple_Pack(2, element_types[symbol], sval);
  if (!result) goto error;

  Py_DECREF(sval);
  return result;

error:
  Py_DECREF(sval);
  return NULL;
}

static PyObject* decode_js_with_scope(BufIter* buf_iter) {
  uint32_t size;
  PyObject* jscode;
  PyObject* mapping;
  PyObject* result;

  jscode = NULL;
  mapping = NULL;

  if (!scan_int32(buf_iter, &size, "js-size-val"))
    return NULL;

  jscode = decode_string(buf_iter);
  if (!jscode) return NULL;

  mapping = decode_document(buf_iter);
  if (!mapping) goto error;

  result = PyTuple_Pack(3, element_types[scoped_js],
                        jscode, mapping);
  if (!result) goto error;

  Py_DECREF(jscode);
  Py_DECREF(mapping);
  return result;

error:
  Py_XDECREF(jscode);
  Py_XDECREF(mapping);
  return NULL;
}

static PyObject* decode_double(BufIter* buf_iter) {
  PyObject* dbl;

  if (!next(buf_iter, 8, "double"))
    return NULL;
  dbl = PyFloat_FromDouble(VAL_AT(buf_iter, double));

  if (!dbl) return NULL;
  return dbl;
}

static PyObject* decode_int32(BufIter* buf_iter) {
  uint32_t val;

  if (!scan_int32(buf_iter, &val, "int32-val"))
    return NULL;

  return PyInt_FromLong(val);
}

static PyObject* decode_int64(BufIter* buf_iter) {
  if (!next(buf_iter, 8, "int64-val"))
    return NULL;

  return _PyLong_FromByteArray(PTR_AT(buf_iter, unsigned char*),
                               8, 1, 1);
}

static PyObject* decode_uint64(BufIter* buf_iter) {
  if (!next(buf_iter, 8, "uint64-val"))
    return NULL;

  return _PyLong_FromByteArray(PTR_AT(buf_iter, unsigned char*),
                               8, 1, 0);
}

static PyObject* decode_timestamp(BufIter* buf_iter) {
  PyObject* tsvalue;
  PyObject* result;

  if (!next(buf_iter, 8, "timestamp-int64-val"))
    return NULL;

  tsvalue = _PyLong_FromByteArray(PTR_AT(buf_iter, unsigned char*),
                                  8, 1, 0);
  if (!tsvalue) return NULL;

  result = PyTuple_Pack(2, element_types[timestamp], tsvalue);
  if (!result) goto error;

  Py_DECREF(tsvalue);
  return result;

error:
  Py_DECREF(tsvalue);
  return NULL;
}

static PyObject* decode_utc(BufIter* buf_iter) {
  PyObject* utcvalue;
  PyObject* result;

  if (!next(buf_iter, 8, "timestamp-int64-val"))
    return NULL;

  utcvalue = _PyLong_FromByteArray(PTR_AT(buf_iter, unsigned char*),
                                  8, 1, 0);
  if (!utcvalue) return NULL;

  result = PyTuple_Pack(2, element_types[utc], utcvalue);
  if (!result) goto error;

  Py_DECREF(utcvalue);
  return result;

error:
  Py_DECREF(utcvalue);
  return NULL;
}

static PyObject* decode_undefined(BufIter* buf_iter) {
  PyObject* undef_str;
  PyObject* result;
  undef_str = PyString_FromString("undefined");
  result = PyTuple_Pack(1, undef_str);
  Py_DECREF(undef_str);
  return result;
}

static PyObject* decode_db_ptr(BufIter* buf_iter){
  PyObject* dbstr;
  PyObject* bytes;
  PyObject* result;
  dbstr = NULL;
  bytes = NULL;

  dbstr = decode_string(buf_iter);
  if (!dbstr) goto error;

  if (!next(buf_iter, 12, "db-ptr-bytes")) goto error;
  bytes = PyString_FromStringAndSize(PTR_AT(buf_iter, char*), 12);
  if (!bytes) goto error;

  result = PyTuple_Pack(3, element_types[db_pointer], dbstr, bytes);
  if (!result) goto error;

  Py_DECREF(dbstr);
  Py_DECREF(bytes);
  return result;

error:
  Py_XDECREF(dbstr);
  Py_XDECREF(bytes);
  return NULL;
}

static decode_element decoders[] = {
  NULL,
  decode_double,           /* 0x01 */
  decode_string,           /* 0x02 */
  decode_document,         /* 0x03 */
  decode_array,            /* 0x04 */
  decode_binary,           /* 0x05 */
  decode_undefined,        /* 0x06 */
  decode_object_id,        /* 0x07 */
  decode_bool,             /* 0x08 */
  decode_utc,              /* 0x09 */
  decode_null,             /* 0x0A */
  decode_regex,            /* 0x0B */
  decode_db_ptr,           /* 0x0C */
  decode_js,               /* 0x0D */
  decode_symbol,           /* 0x0E */
  decode_js_with_scope,    /* 0x0F */
  decode_int32,            /* 0x10 */
  decode_timestamp,        /* 0x11 */
  decode_int64,            /* 0x12 */
};

/* ----------------------------------- encoders ----------------------------------- */
/* TODO(kgm): Be more paranoid about overflow. */
/* TODO(kgm): Code will work by coincidence on 64-bit when e.g. memcpy-ing sizes. */
/* Code will fail horribly on big-endian architecture; probably not a problem. */

#define MAX_BSON_DEPTH 1000

static PyObject* _encode_element(PyObject* key, PyObject* value, int depth);
static PyObject* encode_document(PyObject* doc, int depth);

static PyObject*
_encode_key(PyObject* key, char** remaining, char first, Py_ssize_t extra) {
  PyObject *ret;
  Py_ssize_t key_len;
  char *s, *key_str;

  key_len = PyString_GET_SIZE(key);
  ret = PyString_FromStringAndSize(NULL, key_len + extra + 2);
  if (ret == NULL) {
    return NULL;
  }
  s = PyString_AS_STRING(ret);
  key_str = PyString_AS_STRING(key);
  s[0] = first;
  memcpy(s + 1, key_str, key_len);
  s[key_len + 1] = 0;
  *remaining = s + key_len + 2;
  return ret;
}

static PyObject*
encode_double(PyObject* key, PyObject* value) {
  PyObject *ret;
  char *s;
  double d;

  ret = _encode_key(key, &s, '\x01', 8);
  if (ret == NULL) {
    return NULL;
  }
  d = PyFloat_AS_DOUBLE(value);
  memcpy(s, &d, 8);
  return ret;
}

static PyObject*
encode_string(PyObject* key, PyObject* value) {
  PyObject *utf8_str;
  Py_ssize_t utf8_len;
  PyObject *ret = NULL;
  char *s, *utf8;

  utf8_str = PyUnicode_AsUTF8String(value);
  if (utf8_str == NULL) {
    return NULL;
  }
  utf8_len = PyString_GET_SIZE(utf8_str) + 1;
  utf8 = PyString_AS_STRING(utf8_str);
  ret = _encode_key(key, &s, '\x02', utf8_len + 4);
  if (ret == NULL) {
    goto Done;
  }
  /* BSON is little-endian, we rely on the native endianness matching */
  memcpy(s, &utf8_len, 4);
  memcpy(s + 4, utf8, utf8_len - 1);
  s[utf8_len + 3] = 0;

Done:
  Py_DECREF(utf8_str);
  return ret;
}

static PyObject*
encode_subdocument(PyObject* key, PyObject* value, int depth) {
  PyObject *ret, *subdoc;
  char *s, *subdoc_str;
  Py_ssize_t subdoc_len;

  subdoc = encode_document(value, depth);
  if (subdoc == NULL) {
    return NULL;
  }
  subdoc_len = PyString_GET_SIZE(subdoc);
  ret = _encode_key(key, &s, '\x03', subdoc_len);
  if (ret == NULL) {
    Py_DECREF(subdoc);
    return NULL;
  }
  subdoc_str = PyString_AS_STRING(subdoc);
  memcpy(s, subdoc_str, subdoc_len);
  Py_DECREF(subdoc);
  return ret;
}

static PyObject*
encode_array(PyObject* key, PyObject* value, int depth) {
  PyObject *ret = NULL, *pieces;
  PyObject *element;
  PyObject *element_key, *element_value;
  char *s, *element_buffer;
  Py_ssize_t value_len, i, total_size, element_size;

  value_len = PyList_GET_SIZE(value);
  pieces = PyTuple_New(value_len);
  if (pieces == NULL) {
    return NULL;
  }

  total_size = 0;
  for (i = 0; i < value_len; ++i) {
    element_key = PyString_FromFormat("%zd", i);
    if (element_key == NULL) {
      goto Done;
    }
    element_value = PyList_GET_ITEM(value, i);
    if (element_value == NULL) {
      Py_DECREF(element_key);
      goto Done;
    }
    element = _encode_element(element_key, element_value, depth);
    Py_DECREF(element_key);
    if (element == NULL) {
      goto Done;
    }
    total_size += PyString_GET_SIZE(element);
    PyTuple_SET_ITEM(pieces, i, element);
  }
  total_size += 5;
  ret = _encode_key(key, &s, '\x04', total_size);
  if (ret == NULL) {
    goto Done;
  }
  memcpy(s, &total_size, 4);
  s += 4;
  for (i = 0; i < value_len; ++i) {
    element = PyTuple_GET_ITEM(pieces, i);
    element_size = PyString_GET_SIZE(element);
    element_buffer = PyString_AS_STRING(element);
    memcpy(s, element_buffer, element_size);
    s += element_size;
  }
  s[0] = 0;

Done:
  Py_DECREF(pieces);
  return ret;
}

static PyObject*
encode_binary(PyObject* key, PyObject* value, char subtype) {
  PyObject *ret;
  char *s, *value_str;
  Py_ssize_t value_size;

  value_size = PyString_GET_SIZE(value);
  value_str = PyString_AS_STRING(value);
  ret = _encode_key(key, &s, '\x05', value_size + 5);
  if (ret == NULL) {
    return NULL;
  }
  memcpy(s, &value_size, 4);
  s[4] = subtype;
  memcpy(s + 5, value_str, value_size);
  return ret;
}

static PyObject*
encode_boolean(PyObject* key, PyObject* value) {
  PyObject *ret;
  char *s;

  ret = _encode_key(key, &s, '\x08', 1);
  if (ret == NULL) {
    return NULL;
  }
  if (value == Py_False) {
    s[0] = 0;
  } else {
    s[0] = 1;
  }
  return ret;
}

/* BSON has a 32-bit integer type and a 64-bit one. Here we simply use the
   smallest one in which the value will fit. */
static PyObject*
encode_integer(PyObject* key, PyObject* value) {
  PyObject *ret;
  char *s;
  long x;
  PY_LONG_LONG y;
  int32_t i32 = 0;
  int64_t i64 = 0;

  if (PyInt_CheckExact(value)) {
    x = PyInt_AS_LONG(value);
    /* In practice we only ever use a 32-bit Python, but there's no reason why
       we can't do this the right way. */
#if LONG_MAX > INT32_MAX
    if (x < INT32_MIN || x > INT32_MAX) {
      i64 = x;
    } else {
      i32 = x;
    }
#else
    i32 = x;
#endif
  } else {
    y = PyLong_AsLongLong(value);
    if (y == -1 && PyErr_Occurred()) {
      return NULL;
    }
    /* This isn't true even on an amd64 build of Python, but there's no harm in
       checking. */
#if PY_LLONG_MAX > INT64_MAX
    if (y < INT64_MIN || y > INT64_MAX) {
      PyErr_SetString(PyExc_OverflowError, "overflow in BSON encoding");
      return NULL;
    }
#endif
    if (y < INT32_MIN || y > INT32_MAX) {
      i64 = y;
    } else {
      i32 = y;
    }
  }
  if (i64 != 0) {
    ret = _encode_key(key, &s, '\x12', 8);
    if (ret == NULL) {
      return NULL;
    }
    memcpy(s, &i64, 8);
  } else {
    ret = _encode_key(key, &s, '\x10', 4);
    if (ret == NULL) {
      return NULL;
    }
    memcpy(s, &i32, 4);
  }
  return ret;
}

static PyObject*
encode_null(PyObject* key) {
  PyObject *ret;
  char *s;

  ret = _encode_key(key, &s, '\x0A', 0);

  return ret;
}

static PyObject*
_encode_element(PyObject* key, PyObject* value, int depth) {
  PyObject *element;
  if (!PyString_CheckExact(key)) {
    PyErr_SetString(PyExc_TypeError, "document keys must be of type str");
    return NULL;
  }
  /* \x01 floating point */
  if (PyFloat_Check(value)) {
    element = encode_double(key, value);
  }
  /* \x02 UTF-8 string */
  else if (PyUnicode_Check(value)) {
    element = encode_string(key, value);
  }
  /* \x03 embedded document */
  else if (PyDict_Check(value)) {
    if (depth >= MAX_BSON_DEPTH) {
      PyErr_SetString(PyExc_ValueError, "object too deeply nested to BSON encode");
      return NULL;
    }
    element = encode_subdocument(key, value, depth + 1);
  }
  /* \x04 embedded array */
  else if (PyList_Check(value)) {
    if (depth >= MAX_BSON_DEPTH) {
      PyErr_SetString(PyExc_ValueError, "object too deeply nested to BSON encode");
      return NULL;
    }
    element = encode_array(key, value, depth + 1);
  }
  /* \x05 binary data */
  else if (PyString_Check(value)) {
    element = encode_binary(key, value, 0);
  }
  /* \x08 boolean */
  else if (value == Py_False || value == Py_True) {
    element = encode_boolean(key, value);
  }
  /* \x10 32-bit integer */
  /* \x12 64-bit integer */
  else if (PyInt_CheckExact(value) || PyLong_CheckExact(value)) {
    element = encode_integer(key, value);
  }
  /* \x0A null value */
  else if (value == Py_None) {
    element = encode_null(key);
  }
  /* \x05 binary data (other) */
  /* \x06 undefined/deprecated */
  /* \x07 ObjectId */
  /* \x09 UTC datetime */
  /* \x0B regular expression */
  /* \x0C DBPointer */
  /* \x0D JavaScript code */
  /* \x0E symbol */
  /* \x0F JavaScript code w/ scope */
  /* \x11 Timestamp */
  /*else if (PyTuple_Check(value)) {
    PyErr_SetString(PyExc_TypeError, "weird types not yet supported");
    return NULL;
  }*/
  else {
    PyErr_SetString(PyExc_TypeError, "unsupported type for BSON encode");
    return NULL;
  }
  return element;
}

static PyObject*
encode_document(PyObject* doc, int depth) {
  Py_ssize_t i, n, pos = 0;
  PyObject *pieces, *result = NULL;
  PyObject *key, *value;
  PyObject *element;
  Py_ssize_t total_size = 0, element_size;
  char *s, *element_buffer;

  if (!PyDict_Check(doc)) {
    PyErr_SetString(PyExc_TypeError, "bson document must be a dict");
    return NULL;
  }
  n = PyObject_Size(doc);
  if (n == -1) {
    return NULL;
  }
  pieces = PyTuple_New(n);
  if (pieces == NULL) {
    return NULL;
  }

  i = 0;
  while (PyDict_Next(doc, &pos, &key, &value)) {
    element = _encode_element(key, value, depth);
    if (element == NULL) {
      goto Done;
    }
    total_size += PyString_GET_SIZE(element);
    PyTuple_SET_ITEM(pieces, i, element);
    ++i;
  }
  total_size += 5;
  result = PyString_FromStringAndSize(NULL, total_size);
  if (result == NULL) {
    goto Done;
  }
  s = PyString_AS_STRING(result);
  memcpy(s, &total_size, 4);
  s += 4;
  for (i = 0; i < n; ++i) {
    element = PyTuple_GET_ITEM(pieces, i);
    element_size = PyString_GET_SIZE(element);
    element_buffer = PyString_AS_STRING(element);
    memcpy(s, element_buffer, element_size);
    s += element_size;
  }
  s[0] = 0;

Done:
  Py_DECREF(pieces);
  return result;
}

static PyObject*
dumps(PyObject* self, PyObject* args) {
  PyObject *doc;

  if (!PyArg_ParseTuple(args, "O:dumps", &doc)) {
    return NULL;
  }
  
  return encode_document(doc, 0);
}

/* -------------------------------------------------------------------- */

PyDoc_STRVAR(loads__doc__,
"loads(buffer) -> obj:dict\n\
\n\
Decodes a BSON buffer and returns the decoded dictionary. \
Buffer may be a string or any buffer object. \
Left-over bytes in buffer are not reported.");

PyDoc_STRVAR(decode_next__doc__,
"decode_next(buffer, offset=0) -> (new_offset:int, obj:dict)\
\n\
Decodes buffer starting at offset until exactly one BSON document is \
decoded. Returns the decoded object and the subsequent offset. \
If the returned offset is equal to the size of the input buffer, \
then no more input is available.\
\n\
When enough bytes are not available, BSONBufferTooShort is raised. The \
second arg of BSONBufferTooShort stores the number of additional \
bytes required for the document.");

PyDoc_STRVAR(dumps__doc__,
"dumps(dict) -> str\n\
\n\
Encodes a dictionary and returns a BSON buffer.");

static struct PyMethodDef cbson_functions[] = {
  {"loads", (PyCFunction) loads, METH_VARARGS,
   loads__doc__},
  {"decode_next", (PyCFunction) decode_next, METH_VARARGS,
   decode_next__doc__},
  {"dumps", (PyCFunction) dumps, METH_VARARGS,
   dumps__doc__},
  {NULL, NULL, 0, NULL} /* sentinel */
};

DL_EXPORT(void)
initcbson(void) {
  PyObject *m;
  int i;
  m = Py_InitModule("cbson", cbson_functions);
  if (m==NULL)
    return;

  BSONError = PyErr_NewException("cbson.BSONError", NULL, NULL);
  if (BSONError == NULL)
    return;
  BSONBufferTooShort = PyErr_NewException("cbson.BSONBufferTooShort", BSONError, NULL);
  if (BSONBufferTooShort == NULL)
    return;

  PyModule_AddObject(m, "BSONError", BSONError);
  PyModule_AddObject(m, "BSONBufferTooShort", BSONBufferTooShort);

  /* make string constants */
  for (i=0; i<sizeof(element_type_names)/sizeof(char *); i++) {
    element_types[i] = PyString_FromString(element_type_names[i]);
    Py_INCREF(element_types[i]);
  };
}
