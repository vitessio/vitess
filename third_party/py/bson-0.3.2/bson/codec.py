#!/usr/bin/python -OOOO
# vim: set fileencoding=utf8 shiftwidth=4 tabstop=4 textwidth=80 foldmethod=marker :
# Copyright (c) 2010, Kou Man Tong. All rights reserved.
# For licensing, see LICENSE file included in the package.
"""
Base codec functions for bson.
"""
import struct
import cStringIO
import calendar
from datetime import datetime
import warnings
from abc import ABCMeta, abstractmethod

# serialization optimizations
length_struct = struct.Struct('<i')
int64_struct = struct.Struct('<q')
uint64_struct = struct.Struct('<Q')
binary_struct = struct.Struct('<ib')
double_struct = struct.Struct('<d')
boolean_struct = struct.Struct('<b')

unpack_length = length_struct.unpack_from
unpack_binary_struct = binary_struct.unpack_from

#  Error Classes
class MissingClassDefinition(ValueError):
	def __init__(self, class_name):
		super(MissingClassDefinition, self).__init__(
		"No class definition for class %s" % (class_name,))
# 
#  Warning Classes
class MissingTimezoneWarning(RuntimeWarning):
	def __init__(self, *args):
		args = list(args)
		if len(args) < 1:
			args.append("Input datetime object has no tzinfo, assuming UTC.")
		super(MissingTimezoneWarning, self).__init__(*args)
# 
#  Traversal Step
class TraversalStep(object):
	def __init__(self, parent, key):
		self.parent = parent
		self.key = key
# 
#  Custom Object Codec

class BSONCoding(object):
	__metaclass__ = ABCMeta

	@abstractmethod
	def bson_encode(self):
		pass

	@abstractmethod
	def bson_init(self, raw_values):
		pass

classes = {}

def import_class(cls):
	if not issubclass(cls, BSONCoding):
		return

	global classes
	classes[cls.__name__] = cls

def import_classes(*args):
	for cls in args:
		import_class(cls)

def import_classes_from_modules(*args):
	for module in args:
		for item in module.__dict__:
			if hasattr(item, "__new__") and hasattr(item, "__name__"):
				import_class(item)

def encode_object(obj, traversal_stack, generator_func):
	values = obj.bson_encode()
	class_name = obj.__class__.__name__
	values["$$__CLASS_NAME__$$"] = class_name
	return encode_document(values, traversal_stack, obj, generator_func)

def encode_object_element(name, value, traversal_stack, generator_func):
	return "\x03" + encode_cstring(name) + \
			encode_object(value, traversal_stack,
					generator_func = generator_func)

class _EmptyClass(object):
	pass

def decode_object(raw_values):
	global classes
	class_name = raw_values["$$__CLASS_NAME__$$"]
	cls = None
	try:
		cls = classes[class_name]
	except KeyError, e:
		raise MissingClassDefinition(class_name)

	retval = _EmptyClass()
	retval.__class__ = cls
	retval.bson_init(raw_values)
	return retval

# 
#  Codec Logic
def encode_string(value):
	value = value.encode("utf8")
	length = len(value)
	return struct.pack("<i%dsb" % (length,), length + 1, value, 0)

def decode_string(data, base):
	length = unpack_length(data, base)[0]
	value = data[base + 4: base + 4 + length - 1]
	return (base + 4 + length, value)

def encode_cstring(value):
	if isinstance(value, unicode):
		value = value.encode("utf8")
	return value + "\x00"

def decode_cstring(data, base):
	end = data.index('\x00', base)
	# NOTE(msolomon) this decode adds a depressing amount of overhead and
	# seems incorrect. Nothing should expect more than a simple cstring.
	# name = data[base:end].decode('utf8')
	name = data[base:end]
	return (end+1, name)

def encode_binary(value):
	return binary_struct.pack(len(value), 0) + value

def decode_binary(data, base):
	length, binary_type = unpack_binary_struct(data, base)
	return (base + 5 + length, data[base + 5:base + 5 + length])

def decode_double(data, base):
	return (base + 8, double_struct.unpack_from(data, base)[0])

def encode_double_element(name, value):
	return "\x01" + encode_cstring(name) + double_struct.pack(value)

def decode_double_element(data, base):
	base, name = decode_cstring(data, base + 1)
	base, value = decode_double(data, base)
	return (base, name, value)

def encode_string_element(name, value):
	return "\x02" + encode_cstring(name) + encode_string(value)

def decode_string_element(data, base):
	base, name = decode_cstring(data, base + 1)
	base, value = decode_string(data, base)
	value = value.decode("utf8")
	return (base, name, value)

def encode_value(name, value, buf, traversal_stack, generator_func):
	if isinstance(value, str):
		buf.write(encode_binary_element(name, value))
	elif isinstance(value, unicode):
		buf.write(encode_string_element(name, value))
	elif isinstance(value, int):
		if value < -0x80000000 or value > 0x7fffffff:
			buf.write(encode_int64_element(name, value))
		else:
			buf.write(encode_int32_element(name, value))
	elif isinstance(value, long):
		if value <= 0x7fffffffffffffff:
			buf.write(encode_int64_element(name, value))
		else:
			buf.write(encode_uint64_element(name, value))
	elif isinstance(value, bool):
		buf.write(encode_boolean_element(name, value))
	elif value is None:
		buf.write(encode_none_element(name, value))
	elif isinstance(value, dict):
		buf.write(encode_document_element(name, value,
			traversal_stack, generator_func))
	elif isinstance(value, (list, tuple)):
		buf.write(encode_array_element(name, value,
			traversal_stack, generator_func))
	elif isinstance(value, float):
		buf.write(encode_double_element(name, value))
	elif isinstance(value, datetime):
		buf.write(encode_UTCdatetime_element(name, value))
	elif isinstance(value, BSONCoding):
		buf.write(encode_object_element(name, value, traversal_stack,
			generator_func))
	else:
		raise ValueError('value has bad type', type(value))

def encode_document(obj, traversal_stack,
		traversal_parent = None,
		generator_func = None):
	buf = cStringIO.StringIO()
	key_iter = obj.iterkeys()
	if generator_func is not None:
		key_iter = generator_func(obj, traversal_stack)
	for name in key_iter:
		value = obj[name]
		traversal_stack.append(TraversalStep(traversal_parent or obj, name))
		encode_value(name, value, buf, traversal_stack, generator_func)
		traversal_stack.pop()
	e_list = buf.getvalue()
	e_list_length = len(e_list)
	return struct.pack("<i%dsb" % (e_list_length,), e_list_length + 4 + 1,
			e_list, 0)

def encode_array(array, traversal_stack,
		traversal_parent = None,
		generator_func = None):
	buf = cStringIO.StringIO()
	for i, value in enumerate(array):
		traversal_stack.append(TraversalStep(traversal_parent or array, i))
		encode_value(str(i), value, buf, traversal_stack, generator_func)
		traversal_stack.pop()
	e_list = buf.getvalue()
	e_list_length = len(e_list)
	return struct.pack("<i%dsb" % (e_list_length,), e_list_length + 4 + 1,
			e_list, 0)


def decode_document(data, base):
	length = unpack_length(data, base)[0]
	end_point = base + length
	base += 4
	retval = {}
	while base < end_point - 1:
		base, name, value = ELEMENT_DISPATCH[data[base]](data, base)
		retval[name] = value
	if "$$__CLASS_NAME__$$" in retval:
		retval = decode_object(retval)
	return (end_point, retval)

def encode_document_element(name, value, traversal_stack, generator_func):
	return "\x03" + encode_cstring(name) + \
			encode_document(value, traversal_stack,
					generator_func = generator_func)

def decode_document_element(data, base):
	base, name = decode_cstring(data, base + 1)
	base, value = decode_document(data, base)
	return (base, name, value)

def encode_array_element(name, value, traversal_stack, generator_func):
	return "\x04" + encode_cstring(name) + \
			encode_array(value, traversal_stack, generator_func = generator_func)


def _decode_array_document(data, base):
	length = unpack_length(data, base)[0]
	end_point = base + length
	base += 4
	retval = []
	while base < end_point - 1:
		base, name, value = ELEMENT_DISPATCH[data[base]](data, base)
		retval.append(value)
	return (end_point, retval)

def decode_array_element(data, base):
	base, name = decode_cstring(data, base + 1)
	base, retval = _decode_array_document(data, base)
	return (base, name, retval)

def encode_binary_element(name, value):
	return "\x05" + encode_cstring(name) + encode_binary(value)

def decode_binary_element(data, base):
	base, name = decode_cstring(data, base + 1)
	base, value = decode_binary(data, base)
	return (base, name, value)

def encode_boolean_element(name, value):
	return "\x08" + encode_cstring(name) + boolean_struct.pack(value)

def decode_boolean_element(data, base):
	base, name = decode_cstring(data, base + 1)
	value = bool(boolean_struct.unpack_from(data, base)[0])
	return (base + 1, name, value)

def encode_UTCdatetime_element(name, value):
	value = int(round(calendar.timegm(value.utctimetuple()) * 1000 +
		(value.microsecond / 1000.0)))
	return "\x09" + encode_cstring(name) + int64_struct.pack(value)

def decode_UTCdatetime_element(data, base):
	base, name = decode_cstring(data, base + 1)
	value = datetime.utcfromtimestamp(
			int64_struct.unpack_from(data, base)[0] / 1000.0)
	return (base + 8, name, value)

def encode_none_element(name, value):
	return "\x0a" + encode_cstring(name)

def decode_none_element(data, base):
	base, name = decode_cstring(data, base + 1)
	return (base, name, None)

def encode_int32_element(name, value):
	return "\x10" + encode_cstring(name) + length_struct.pack(value)

def decode_int32_element(data, base):
	base, name = decode_cstring(data, base + 1)
	value = unpack_length(data, base)[0]
	return (base + 4, name, value)

def encode_int64_element(name, value):
	return "\x12" + encode_cstring(name) + int64_struct.pack(value)

def encode_uint64_element(name, value):
	return "\x3F" + encode_cstring(name) + uint64_struct.pack(value)

def decode_int64_element(data, base):
	base, name = decode_cstring(data, base + 1)
	value = int64_struct.unpack_from(data, base)[0]
	return (base + 8, name, value)

def decode_uint64_element(data, base):
	base, name = decode_cstring(data, base + 1)
	value = uint64_struct.unpack_from(data, base)[0]
	return (base + 8, name, value)

ELEMENT_TYPES = {
		0x01 : "double",
		0x02 : "string",
		0x03 : "document",
		0x04 : "array",
		0x05 : "binary",
		0x08 : "boolean",
		0x09 : "UTCdatetime",
		0x0A : "none",
		0x10 : "int32",
		0x12 : "int64",
		0x3F : "uint64"
	}

# optimize dispatch once all methods are known
ELEMENT_DISPATCH = dict([(chr(i), globals()["decode_" + name + "_element"])
		    for i, name in ELEMENT_TYPES.iteritems()])
