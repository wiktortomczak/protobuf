"""Contains routines for printing protocol messages in JS object format.

Note: This is *not* JSON format, but the format compatible with
fromObject() toObject() of the JS runtime library.

See src/google/protobuf/compiler/js/js_generator.cc.

Adapted from json_format.py.

Simple usage example:

  # Create a proto object and serialize it to JS object string.
  message = my_proto_pb2.MyMessage(foo='bar')
  js_object_string = js_object_format.MessageToJsObject(message)

  # Parse a proto in JS object string format to proto object.
  message = js_object_format.Parse(js_object_string, my_proto_pb2.MyMessage())
"""

try:
  from collections import OrderedDict
except ImportError:
  from ordereddict import OrderedDict  #PY26
import base64
import json
import math
import re
import six
import sys

from google.protobuf import descriptor

_TIMESTAMPFOMAT = '%Y-%m-%dT%H:%M:%S'
_INT_TYPES = frozenset([descriptor.FieldDescriptor.CPPTYPE_INT32,
                        descriptor.FieldDescriptor.CPPTYPE_UINT32,
                        descriptor.FieldDescriptor.CPPTYPE_INT64,
                        descriptor.FieldDescriptor.CPPTYPE_UINT64])
_INT64_TYPES = frozenset([descriptor.FieldDescriptor.CPPTYPE_INT64,
                          descriptor.FieldDescriptor.CPPTYPE_UINT64])
_FLOAT_TYPES = frozenset([descriptor.FieldDescriptor.CPPTYPE_FLOAT,
                          descriptor.FieldDescriptor.CPPTYPE_DOUBLE])
_INFINITY = 'Infinity'
_NEG_INFINITY = '-Infinity'
_NAN = 'NaN'

_UNPAIRED_SURROGATE_PATTERN = re.compile(six.u(
    r'[\ud800-\udbff](?![\udc00-\udfff])|(?<![\ud800-\udbff])[\udc00-\udfff]'
))

class Error(Exception):
  """Top-level module error for js_object_format."""


class SerializeToJsObjectError(Error):
  """Thrown if serialization to JS object fails."""


class ParseError(Error):
  """Thrown in case of parsing error."""


def MessageToJsObject(message):
  """Converts protobuf message to JS object format.

  Args:
    message: The protocol buffers message instance to serialize.
  Returns:
    A string containing the JS object formatted protocol buffer message.
  """
  return _Printer().ToJsObjectString(message)


def MessageToDict(message):
  """Converts protobuf message to a JS object dictionary.

  Args:
    message: The protocol buffers message instance to serialize.
  Returns:
    A dict representation of the JS object formatted protocol buffer message.
  """
  # pylint: disable=protected-access
  return _Printer()._MessageToJsObject(message)


def _IsMapEntry(field):
  return (field.type == descriptor.FieldDescriptor.TYPE_MESSAGE and
          field.message_type.has_options and
          field.message_type.GetOptions().map_entry)


class _Printer(object):
  """JS object format printer for protocol message."""

  def ToJsObjectString(self, message):
    js = self._MessageToJsObject(message)
    return json.dumps(js, indent=2)

  def _MessageToJsObject(self, message):
    """Converts message to an object according to JS object specification."""
    message_descriptor = message.DESCRIPTOR
    full_name = message_descriptor.full_name
    if _IsWrapperMessage(message_descriptor):
      raise NotImplementedError('wrapper message')
    if full_name in _WELL_KNOWN_TYPES:
      raise NotImplementedError('well known types')
    js = {}
    return self._RegularMessageToJsObject(message, js)

  def _RegularMessageToJsObject(self, message, js):
    """Converts normal message according to JS object specification."""
    fields = message.ListFields()

    try:
      for field, value in fields:
        name = _GetJSObjectFieldName(field)
        if _IsMapEntry(field):
          # Convert a map field.
          js_entries = []
          for key in value:
            if isinstance(key, bool):
              if key:
                recorded_key = 'true'
              else:
                recorded_key = 'false'
            else:
              recorded_key = key

            v_field = field.message_type.fields_by_name['value']
            recorded_value = self._FieldToJsObject(v_field, value[key])

            js_entries.append([recorded_key, recorded_value])
          js[name] = js_entries
        elif field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
          # Convert a repeated field.
          js[name] = [self._FieldToJsObject(field, k)
                      for k in value]
        else:
          js[name] = self._FieldToJsObject(field, value)

    except ValueError as e:
      raise SerializeToJsObjectError(
          'Failed to serialize {0} field: {1}.'.format(field.name, e))

    return js

  def _FieldToJsObject(self, field, value):
    """Converts field value according to JS object specification."""
    if field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_MESSAGE:
      return self._MessageToJsObject(value)
    elif field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_ENUM:
      return value
    elif field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_STRING:
      if field.type == descriptor.FieldDescriptor.TYPE_BYTES:
        # Use base64 Data encoding for bytes
        return base64.b64encode(value).decode('utf-8')
      else:
        return value
    elif field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_BOOL:
      return bool(value)
    elif field.cpp_type in _INT64_TYPES:
      return str(value)
    elif field.cpp_type in _FLOAT_TYPES:
      if math.isinf(value):
        if value < 0.0:
          return _NEG_INFINITY
        else:
          return _INFINITY
      if math.isnan(value):
        return _NAN
    return value


def _GetJSObjectFieldName(field):
  """Returns field name in JS object format.

  Args:
    field: FieldDescriptor.
  Returns:
    str.
  """
  if _IsMapEntry(field):
    return field.json_name + 'Map'
  elif field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
    return field.json_name + 'List'
  else:
    return field.json_name


def _IsWrapperMessage(message_descriptor):
  return message_descriptor.file.name == 'google/protobuf/wrappers.proto'


def _DuplicateChecker(js):
  result = {}
  for name, value in js:
    if name in result:
      raise ParseError('Failed to load JS object: duplicate key {0}.'.format(name))
    result[name] = value
  return result


def Parse(text, message, ignore_unknown_fields=False):
  """Parses a JS object representation of a protocol message into a message.

  Args:
    text: Message JS object representation.
    message: A protocol buffer message to merge into.
    ignore_unknown_fields: If True, do not raise errors for unknown fields.

  Returns:
    The same message passed as argument.

  Raises::
    ParseError: On JS object parsing problems.
  """
  if not isinstance(text, six.text_type): text = text.decode('utf-8')
  try:
    if sys.version_info < (2, 7):
      # object_pair_hook is not supported before python2.7
      js = json.loads(text)
    else:
      js = json.loads(text, object_pairs_hook=_DuplicateChecker)
  except ValueError as e:
    raise ParseError('Failed to load JS object: {0}.'.format(str(e)))
  return ParseDict(js, message, ignore_unknown_fields)


def ParseDict(js_dict, message, ignore_unknown_fields=False):
  """Parses a JS object dictionary representation into a message.

  Args:
    js_dict: Dict representation of a JS object message.
    message: A protocol buffer message to merge into.
    ignore_unknown_fields: If True, do not raise errors for unknown fields.

  Returns:
    The same message passed as argument.
  """
  raise NotImplementedError(
    'Deserialization from JS object format is not implemented. '
    'Only serialization to JS object format is implemented')
  parser = _Parser(ignore_unknown_fields)
  parser.ConvertMessage(js_dict, message)
  return message


_INT_OR_FLOAT = six.integer_types + (float,)


class _Parser(object):
  """JS object format parser for protocol message."""

  def __init__(self,
               ignore_unknown_fields):
    self.ignore_unknown_fields = ignore_unknown_fields

  def ConvertMessage(self, value, message):
    """Convert a JS object object into a message.

    Args:
      value: A JS object object.
      message: A WKT or regular protocol message to record the data.

    Raises:
      ParseError: In case of convert problems.
    """
    message_descriptor = message.DESCRIPTOR
    full_name = message_descriptor.full_name
    if _IsWrapperMessage(message_descriptor):
      raise NotImplementedError('wrapper message')
    elif full_name in _WELL_KNOWN_TYPES:
      raise NotImplementedError('well known types')
    else:
      self._ConvertFieldValuePair(value, message)

  def _ConvertFieldValuePair(self, js, message):
    """Convert field value pairs into regular message.

    Args:
      js: A JS object object to convert the field value pairs.
      message: A regular protocol message to record the data.

    Raises:
      ParseError: In case of problems converting.
    """
    names = []
    message_descriptor = message.DESCRIPTOR
    fields_by_json_name = dict((_GetJSObjectFieldName(f), f)
                               for f in message_descriptor.fields)
    for name in js:
      try:
        field = fields_by_json_name.get(name, None)
        if not field:
          field = message_descriptor.fields_by_name.get(name, None)
        if not field:
          if self.ignore_unknown_fields:
            continue
          raise ParseError(
              'Message type "{0}" has no field named "{1}".'.format(
                  message_descriptor.full_name, name))
        if name in names:
          raise ParseError('Message type "{0}" should not have multiple '
                           '"{1}" fields.'.format(
                               message.DESCRIPTOR.full_name, name))
        names.append(name)
        # Check no other oneof field is parsed.
        if field.containing_oneof is not None:
          oneof_name = field.containing_oneof.name
          if oneof_name in names:
            raise ParseError('Message type "{0}" should not have multiple '
                             '"{1}" oneof fields.'.format(
                                 message.DESCRIPTOR.full_name, oneof_name))
          names.append(oneof_name)

        value = js[name]
        if value is None:
          if (field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_MESSAGE
              and field.message_type.full_name == 'google.protobuf.Value'):
            sub_message = getattr(message, field.name)
            sub_message.null_value = 0
          else:
            message.ClearField(field.name)
          continue

        # Parse field value.
        if _IsMapEntry(field):
          message.ClearField(field.name)
          self._ConvertMapFieldValue(value, message, field)
        elif field.label == descriptor.FieldDescriptor.LABEL_REPEATED:
          message.ClearField(field.name)
          if not isinstance(value, list):
            raise ParseError('repeated field {0} must be in [] which is '
                             '{1}.'.format(name, value))
          if field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_MESSAGE:
            # Repeated message field.
            for item in value:
              sub_message = getattr(message, field.name).add()
              # None is a null_value in Value.
              if (item is None and
                  sub_message.DESCRIPTOR.full_name != 'google.protobuf.Value'):
                raise ParseError('null is not allowed to be used as an element'
                                 ' in a repeated field.')
              self.ConvertMessage(item, sub_message)
          else:
            # Repeated scalar field.
            for item in value:
              if item is None:
                raise ParseError('null is not allowed to be used as an element'
                                 ' in a repeated field.')
              getattr(message, field.name).append(
                  _ConvertScalarFieldValue(item, field))
        elif field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_MESSAGE:
          sub_message = getattr(message, field.name)
          sub_message.SetInParent()
          self.ConvertMessage(value, sub_message)
        else:
          setattr(message, field.name, _ConvertScalarFieldValue(value, field))
      except ParseError as e:
        if field and field.containing_oneof is None:
          raise ParseError('Failed to parse {0} field: {1}'.format(name, e))
        else:
          raise ParseError(str(e))
      except ValueError as e:
        raise ParseError('Failed to parse {0} field: {1}.'.format(name, e))
      except TypeError as e:
        raise ParseError('Failed to parse {0} field: {1}.'.format(name, e))


  def _ConvertMapFieldValue(self, value, message, field):
    """Convert map field value for a message map field.

    Args:
      value: A JS object object to convert the map field value.
      message: A protocol message to record the converted data.
      field: The descriptor of the map field to be converted.

    Raises:
      ParseError: In case of convert problems.
    """
    if not isinstance(value, dict):
      raise ParseError(
          'Map field {0} must be in a dict which is {1}.'.format(
              field.name, value))
    key_field = field.message_type.fields_by_name['key']
    value_field = field.message_type.fields_by_name['value']
    for key in value:
      key_value = _ConvertScalarFieldValue(key, key_field, True)
      if value_field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_MESSAGE:
        self.ConvertMessage(value[key], getattr(
            message, field.name)[key_value])
      else:
        getattr(message, field.name)[key_value] = _ConvertScalarFieldValue(
            value[key], value_field)


def _ConvertScalarFieldValue(value, field, require_str=False):
  """Convert a single scalar field value.

  Args:
    value: A scalar value to convert the scalar field value.
    field: The descriptor of the field to convert.
    require_str: If True, the field value must be a str.

  Returns:
    The converted scalar field value

  Raises:
    ParseError: In case of convert problems.
  """
  if field.cpp_type in _INT_TYPES:
    return _ConvertInteger(value)
  elif field.cpp_type in _FLOAT_TYPES:
    return _ConvertFloat(value)
  elif field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_BOOL:
    return _ConvertBool(value, require_str)
  elif field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_STRING:
    if field.type == descriptor.FieldDescriptor.TYPE_BYTES:
      return base64.b64decode(value)
    else:
      # Checking for unpaired surrogates appears to be unreliable,
      # depending on the specific Python version, so we check manually.
      if _UNPAIRED_SURROGATE_PATTERN.search(value):
        raise ParseError('Unpaired surrogate')
      return value
  elif field.cpp_type == descriptor.FieldDescriptor.CPPTYPE_ENUM:
    # Convert an enum value.
    enum_value = field.enum_type.values_by_name.get(value, None)
    if enum_value is None:
      try:
        number = int(value)
        enum_value = field.enum_type.values_by_number.get(number, None)
      except ValueError:
        raise ParseError('Invalid enum value {0} for enum type {1}.'.format(
            value, field.enum_type.full_name))
      if enum_value is None:
        raise ParseError('Invalid enum value {0} for enum type {1}.'.format(
            value, field.enum_type.full_name))
    return enum_value.number


def _ConvertInteger(value):
  """Convert an integer.

  Args:
    value: A scalar value to convert.

  Returns:
    The integer value.

  Raises:
    ParseError: If an integer couldn't be consumed.
  """
  if isinstance(value, float) and not value.is_integer():
    raise ParseError('Couldn\'t parse integer: {0}.'.format(value))

  if isinstance(value, six.text_type) and value.find(' ') != -1:
    raise ParseError('Couldn\'t parse integer: "{0}".'.format(value))

  return int(value)


def _ConvertFloat(value):
  """Convert an floating point number."""
  if value == 'nan':
    raise ParseError('Couldn\'t parse float "nan", use "NaN" instead.')
  try:
    # Assume Python compatible syntax.
    return float(value)
  except ValueError:
    # Check alternative spellings.
    if value == _NEG_INFINITY:
      return float('-inf')
    elif value == _INFINITY:
      return float('inf')
    elif value == _NAN:
      return float('nan')
    else:
      raise ParseError('Couldn\'t parse float: {0}.'.format(value))


def _ConvertBool(value, require_str):
  """Convert a boolean value.

  Args:
    value: A scalar value to convert.
    require_str: If True, value must be a str.

  Returns:
    The bool parsed.

  Raises:
    ParseError: If a boolean value couldn't be consumed.
  """
  if require_str:
    if value == 'true':
      return True
    elif value == 'false':
      return False
    else:
      raise ParseError('Expected "true" or "false", not {0}.'.format(value))

  if not isinstance(value, bool):
    raise ParseError('Expected true or false without quotes.')
  return value


# Special proto messages built into protobuf libraries.
_WELL_KNOWN_TYPES = set([
  'google.protobuf.Any',
  'google.protobuf.Duration',
  'google.protobuf.FieldMask',
  'google.protobuf.ListValue',
  'google.protobuf.Struct',
  'google.protobuf.Timestamp',
  'google.protobuf.Value'
])
