#!/usr/bin/python

# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
"""

import json
import os
import optparse
import pprint
import re

#def print_api_summary(doc, service_summary):
#  doc.write(service_summary + '\n\n')

def print_method_summary(doc, proto_contents, methods):
  for method in sorted(methods, key=lambda k: k['name']):
    method_group_info = method['comment'].split(' API group: ')
    if len(method_group_info) > 1:
      method['group'] = method_group_info[1]
      method['comment'] = method_group_info[0]
    else:
      method['group'] = 'Uncategorized'

  doc.write('This document describes Vitess API methods that enable your ' +
            'client application to more easily talk to your storage system ' +
            'to query data. API methods are grouped into the following ' +
            'categories:\n\n')
  last_group = ''
  for group in proto_contents['group-ordering']:
    for method in sorted(methods, key=lambda k: (k['group'], k['name'])):
      if (group.lower() == method['group'].lower() and
          not method['group'] == last_group):
        if not method['group'] == last_group:
          doc.write('* [' + method['group'] + ']' +
                    '(#' + method['group'].replace(' ', '-').lower() + ')\n')
          last_group = method['group']
  doc.write('\n\n')
    
  doc.write('The following table lists the methods in each group and links ' +
            'to more detail about each method:\n\n')
  doc.write('<table id="api-method-summary">\n')

  last_group = ''
  for group in proto_contents['group-ordering']:
    for method in sorted(methods, key=lambda k: (k['group'], k['name'])):
      if (group.lower() == method['group'].lower() and
          not method['group'] == last_group):
        print_method_summary_group_row(doc, method['group'])
        last_group = method['group']
        print_method_summary_row(doc, method)
      elif group.lower() == method['group'].lower():
        print_method_summary_row(doc, method)
  doc.write('</table>\n')

def print_method_summary_group_row(doc, group_name):
  doc.write('<tr><td class="api-method-summary-group" colspan="2">' +
            group_name + '</td></tr>\n')

def print_method_summary_row(doc, method):
  doc.write('<tr>\n')
  doc.write('<td><code><a href="#' + method['name'].lower() + '">' +
            method['name'] + '</a></code></td>\n<td>')
  if 'comment' in method and method['comment']:
    doc.write(method['comment'])
  doc.write('</td>\n')
  doc.write('</tr>\n')

def recursively_add_objects(new_objects, method_file, obj,
                            properties, proto_contents):
  if obj in new_objects:
    return

  [op_enum_file, op_enum] = get_op_item(proto_contents, obj, 'enums')
  [op_file, op_method] = get_op_item(proto_contents, obj, 'messages')

  if properties:
    if method_file not in new_objects:
      new_objects[method_file] = {'messages': {}}
    elif 'messages' not in new_objects[method_file]:
      new_objects[method_file]['messages'] = {}
    new_objects[method_file]['messages'][obj] = 1
    for prop in properties:
      type_list = prop['type'].split('.')
      if len(type_list) == 1:
        if (method_file in proto_contents and
            'messages' in proto_contents[method_file]):
          lil_pc = proto_contents[method_file]['messages']
          if type_list[0] in lil_pc and 'properties' in lil_pc[type_list[0]]:
            new_objects = recursively_add_objects(new_objects, method_file,
                prop['type'], lil_pc[type_list[0]]['properties'],
                proto_contents)
          elif (obj in lil_pc and
                'messages' in lil_pc[obj] and
                type_list[0] in lil_pc[obj]['messages'] and
                'properties' in lil_pc[obj]['messages'][type_list[0]]):
            new_objects = recursively_add_objects(new_objects, method_file,
                prop['type'],
                lil_pc[obj]['messages'][type_list[0]]['properties'],
                proto_contents)
      else:
        [op_file, op_method] = get_op_item(proto_contents, obj, 'messages')
        [op_enum_file, op_enum] = get_op_item(proto_contents, obj, 'enums')

        if (op_file and 
            op_file in proto_contents and
            'messages' in proto_contents[op_file] and
            type_list[1] in proto_contents[op_file]['messages'] and
            'properties' in proto_contents[op_file]['messages'][type_list[1]]):
          new_objects = recursively_add_objects(new_objects, op_file, type_list[1],
              proto_contents[op_file]['messages'][type_list[1]]['properties'],
              proto_contents)
        elif (op_enum_file and 
              op_enum_file in proto_contents and
              'enums' in proto_contents[op_enum_file] and
              type_list[1] in proto_contents[op_enum_file]['enums']):
          if not op_enum_file in new_objects['enums']:
            new_objects['enums'][op_enum_file] = {}
          new_objects['enums'][op_enum_file][type_list[1]] = (
              proto_contents[op_enum_file]['enums'][type_list[1]])
  return new_objects

def print_method_details(doc, proto_contents, proto, methods, objects):

  last_group = ''
  for group in proto_contents['group-ordering']:
    for method in sorted(methods, key=lambda k: (k['group'], k['name'])):
      if (group.lower() == method['group'].lower() and
          not method['group'] == last_group):
        doc.write('##' + method['group'] + '\n')
        last_group = method['group']
        print_method_detail_header(doc, method)
        print_method_detail_request(doc, proto_contents, proto, method)
        print_method_detail_response(doc, proto_contents, proto, method)
      elif group.lower() == method['group'].lower():
        print_method_detail_header(doc, method)
        print_method_detail_request(doc, proto_contents, proto, method)
        print_method_detail_response(doc, proto_contents, proto, method)
  new_objects = {}
  for obj in sorted(objects):
    type_list = obj.split('.')
    if len(type_list) == 1:
      method_file = objects[obj]['methods'][0]['method_file']
      if (method_file in proto_contents and
          'messages' in proto_contents[method_file] and
          obj in proto_contents[method_file]['messages'] and
          'properties' in proto_contents[method_file]['messages'][obj]):
        new_objects = recursively_add_objects(new_objects, method_file, obj,
            proto_contents[method_file]['messages'][obj]['properties'],
            proto_contents)

    else:
      [op_file, op_method] = get_op_item(proto_contents, obj, 'messages')
      [op_enum_file, op_enum] = get_op_item(proto_contents, obj, 'enums')
      if (op_file and 
          op_file in proto_contents and
          'messages' in proto_contents[op_file] and
          type_list[1] in proto_contents[op_file]['messages'] and
          'properties' in proto_contents[op_file]['messages'][type_list[1]]):
        new_objects = recursively_add_objects(new_objects, op_file, type_list[1],
            proto_contents[op_file]['messages'][type_list[1]]['properties'],
            proto_contents)
      elif (op_enum_file and 
            op_enum_file in proto_contents and
            'enums' in proto_contents[op_enum_file] and
            type_list[1] in proto_contents[op_enum_file]['enums']):
        if not op_enum_file in new_objects:
          new_objects[op_enum_file] = {'enums':{}}
        elif not 'enums' in new_objects[op_enum_file]:
          new_objects[op_enum_file]['enums'] = {}
        new_objects[op_enum_file]['enums'][type_list[1]] = (
            proto_contents[op_enum_file]['enums'][type_list[1]])

  #print json.dumps(new_objects, sort_keys=True, indent=2)
  print_nested_objects(doc, new_objects, proto_contents)

def print_nested_objects(doc, objects, proto_contents):
  doc.write('## Enums\n\n')
  for obj in sorted(objects):
    if obj == 'vtgate.proto':
      print_proto_enums(doc, proto_contents, obj, objects,
                        {'strip-proto-name': 1})
  for obj in sorted(objects):
    if not obj == 'vtgate.proto':
      print_proto_enums(doc, proto_contents, obj, objects, {})
  doc.write('## Messages\n\n')
  for obj in sorted(objects):
    if obj == 'vtgate.proto':
      print_proto_messages(doc, proto_contents, obj, objects,
                           {'strip-proto-name': 1})
  for obj in sorted(objects):
    if not obj == 'vtgate.proto':
      print_proto_messages(doc, proto_contents, obj, objects, {})

def print_message_detail_header(doc, proto, message_details, message_name,
                                options):
  header_size = '###'
  if 'header-size' in options:
    header_size = options['header-size']

  message = (proto.replace('.proto', '').replace('.pb.go', '') +
             '.' + message_name)
  if (options and
      'strip-proto-name' in options and
      options['strip-proto-name']):
    message = message_name
  elif options and 'add-method-name' in options and 'method-name' in options:
    message = options['method-name'] + '.' + message_name
  doc.write(header_size + ' ' + message + '\n\n')

  if 'comment' in message_details and message_details['comment']:
    doc.write(message_details['comment'].strip() + '\n\n')

def print_method_detail_header(doc, method):
  doc.write('### ' + method['name'] + '\n\n')
  if 'comment' in method and method['comment']:
    doc.write(method['comment'] + '\n\n')

def print_properties_header(doc, header, table_headers):
  if header:
    doc.write('##### ' + header + '\n\n')
  if table_headers:
    doc.write('| ')
    for field in table_headers:
      doc.write(field + ' |')
    doc.write('\n')
    for field in table_headers:
      doc.write('| :-------- ')
    doc.write('\n')

def print_property_row(doc, proto_contents, proto, method_file, method, prop):
  # Print property/parameter name
  if 'name' in prop:
    doc.write('| <code>' + prop['name'] + '</code> ')

  method_in_messages = False
  enum_in_messages = False
  for key in proto_contents[proto]['messages']:
    if key == method:
      method_in_messages = True
  for key in proto_contents[proto]['enums']:
    if key == prop['type']:
      enum_in_messages = True

  # Print property/parameter type
  if 'type' in prop and prop['type']:
    doc.write('<br>')
    prop_text = ''
    if prop['type'][0:5] == 'map <':
      map_value = prop['type'].split(',')[1].split('>')[0].strip()
      if (map_value and
          map_value in proto_contents[proto]['messages']):
        prop_text = (prop['type'].split(',')[0] + ', [' + map_value + ']' +
                     '(#' + proto.lower().replace('.proto', '') + '.' +
                     map_value.lower() + ')' + '>')
        prop_text = prop_text.replace('<', '&lt;').replace('>', '&gt;')
    else:
      type_list = prop['type'].split('.')
      if len(type_list) == 2:
        prop_text = ('[' + prop['type'] + '](#' + type_list[0] + '.' +
                     type_list[1].lower() + ')')
      elif (method_file and
            prop['type'] in proto_contents[method_file]['messages']):
        if method_file == 'vtgate.proto':
          prop_text = '[' + prop['type'] + '](#' + prop['type'].lower() + ')'
        else:
          prop_text = ('[' + prop['type'] +
                       '](#' + proto.lower().replace('.proto', '') + '.' +
                       prop['type'].lower() + ')')
      elif enum_in_messages:
        prop_text = ('[' + prop['type'] + ']' +
                     '(#' + proto.lower().replace('.proto', '') + '.' +
                     prop['type'].lower() + ')')
      elif (method_file and
            prop['type'] in proto_contents[method_file]['enums']):
        prop_text = '[' + prop['type'] + '](#' + prop['type'].lower() + ')'
      elif (method_in_messages and
            'messages' in proto_contents[proto]['messages'][method]):
        if prop['type'] in proto_contents[proto]['messages'][method]['messages']:
          prop_text = '[' + prop['type'] + '](#' + method.lower() + '.' + prop['type'].lower() + ')'
        elif prop['type'] in proto_contents[proto]['messages'][method]['enums']:
          prop_text = '[' + prop['type'] + '](#' + method.lower() + '.' + prop['type'].lower() + ')'
        else:
          prop_text = prop['type']

      else:
        prop_text = prop['type']
        bad_text = True
        for p in proto_contents:
          if 'messages' in proto_contents[p] and proto_contents[p]['messages']:
            for m in proto_contents[p]['messages']:
              if ('messages' in proto_contents[p]['messages'][m] and
                  proto_contents[p]['messages'][m]['messages'] and
                  prop['type'] in proto_contents[p]['messages'][m]['messages']):
                prop_text = ('[' + prop['type'] + ']' +
                             '(#' + m.lower() + '.' + prop['type'].lower() + ')')
                bad_text = False
        if bad_text and isinstance(method, basestring):
          for p in proto_contents:
            if ('messages' in proto_contents[p] and
                proto_contents[p]['messages']):
              for m in proto_contents[p]['messages']:
                if ('messages' in proto_contents[p]['messages'][m] and
                    proto_contents[p]['messages'][m]['messages'] and
                    method in proto_contents[p]['messages'][m]['messages'] and
                    'enums' in proto_contents[p]['messages'][m]['messages'][method] and
                    prop['type'] in proto_contents[p]['messages'][m]['messages'][method]['enums']):
                  prop_text = ('[' + prop['type'] + ']' +
                               '(#' + m.lower() + '.' + method.lower() + '.' +
                               prop['type'].lower() + ')')

    if 'status' in prop and prop['status'] == 'repeated':
      prop_text = 'list &lt;' + prop_text + '&gt;'
    doc.write(prop_text)

  # Print property/parameter definition.

  if 'type' in prop and prop['type']:
    # If type contains period -- e.g. vtrpc.CallerId -- then it refers to
    # a message in another proto. We want to print the comment identifying
    # that message.  In that case, the link field should also link to a doc
    # for that proto.
    type_list = prop['type'].split('.')
    [op_file, op_method] = get_op_item(proto_contents, prop['type'],
                                         'messages')
    [op_enum_file, op_enum] = get_op_item(proto_contents, prop['type'], 'enums')
    if op_method:
      doc.write('| ' + op_method['comment'].strip())
    elif op_enum:
      doc.write('| ' + op_enum['comment'].strip())
    elif (method_file and
          prop['type'] in proto_contents[method_file]['messages']):
      if ('comment' in proto_contents[method_file]['messages'][prop['type']] and
          proto_contents[method_file]['messages'][prop['type']]['comment']):
        doc.write('| ' +
            proto_contents[method_file]['messages'][prop['type']]['comment'].strip())
      else:
        doc.write('|')
    elif 'comment' in prop and prop['comment']:
      doc.write('| ' + prop['comment'].strip())
    else:
      doc.write('|')
  elif 'comment' in prop and prop['comment']:
    doc.write('| ' + prop['comment'].strip())
  else:
    doc.write('|')
  doc.write(' |\n')

def get_op_item(proto_contents, item, item_type):
  item_list = item.split('.')
  if len(item_list) == 2:
    item_file = item_list[0] + '.proto'
    item_enum = item_list[1]
    if item_file in proto_contents:
      if item_type in proto_contents[item_file]:
        if item_enum in proto_contents[item_file][item_type]:
          return item_file, proto_contents[item_file][item_type][item_enum]
        else:
          return [None, None]
      else:
        return [None, None]
    else:
      return [None, None]
  else:
    return [None, None]

def print_method_detail_request(doc, proto_contents, proto, method):
  if method['request']:
    [op_file, op_method] = get_op_item(proto_contents, method['request'],
                                       'messages')
    if (op_method and
        'comment' in op_method and
        op_method['comment']):
      doc.write('#### Request\n\n')
      doc.write(op_method['comment'] + '\n\n')
      print_properties_header(doc, 'Parameters', ['Name', 'Description'])
      for prop in op_method['properties']:
        print_property_row(doc, proto_contents, proto, op_file, op_method, prop)
      doc.write('\n')

      if 'messages' in op_method and op_method['messages']:
        doc.write('#### Messages\n\n')
        for message in sorted(op_method['messages']):
          print_proto_message(doc, proto, proto_contents,
                              op_method['messages'][message], message,
                              {'header-size': '#####',
                               'add-method-name': 1,
                               'method-name': method['request'].split('.')[1]})

def print_method_detail_response(doc, proto_contents, proto, method):
  if method['response']:
    [op_file, op_method] = get_op_item(proto_contents,
        method['response'].replace('stream ', ''), 'messages')
    if (op_method and
        'comment' in op_method and
        op_method['comment']):
      doc.write('#### Response\n\n')
      doc.write(op_method['comment'] + '\n\n')
      print_properties_header(doc, 'Properties', ['Name', 'Description'])
      for prop in op_method['properties']:
        print_property_row(doc, proto_contents, proto, op_file, op_method, prop)
      doc.write('\n')

      if 'messages' in op_method and op_method['messages']:
        doc.write('#### Messages\n\n')
        for message in sorted(op_method['messages']):
          print_proto_message(doc, proto, proto_contents,
                              op_method['messages'][message], message,
                              {'header-size': '#####',
                               'add-method-name': 1,
                               'method-name': method['response'].split('.')[1]})

def print_proto_file_definition(doc, proto_contents, proto):
  if ('file_definition' in proto_contents[proto] and
      proto_contents[proto]['file_definition']):
    doc.write(proto_contents[proto]['file_definition'].strip() + '\n\n')

def print_proto_enum(doc, enum_details, enum_name, proto, options):
  header_size = '###'
  if 'header-size' in options:
    header_size = options['header-size']
  # Print name of enum as header
  enum_header = proto.replace('.proto', '') + '.' + enum_name
  if options and 'add-method-name' in options and 'method-name' in options:
    enum_header = options['method-name'] + '.' + enum_name
  elif (options and
        'strip-proto-name' in options and
        options['strip-proto-name']):
    enum_header = enum_name

  doc.write(header_size + ' ' + enum_header + '\n\n')

  if 'comment' in enum_details and enum_details['comment']:
    doc.write(enum_details['comment'] + '\n\n')

  print_properties_header(doc, None, ['Name', 'Value', 'Description'])
  for value in enum_details['values']:
    # Print enum text
    if 'text' in value:
      doc.write('| <code>' + value['text'] + '</code> ')
    else:
      doc.write('| ')

    # Print enum value
    if 'value' in value and value['value']:
      doc.write('| <code>' + value['value'] + '</code> ')
    else:
      doc.write('| ')

    # Print enum value description
    if 'comment' in value and value['comment']:
      doc.write('| ' + value['comment'].strip() + ' ')
    else:
      doc.write('| ')

    doc.write(' |\n')
  doc.write('\n')

def print_proto_message(doc, proto, proto_contents, message_details, message,
                        options):

  print_message_detail_header(doc, proto, message_details, message, options)

  if 'header-size' in options:
    doc.write('<em>Properties</em>\n\n')
  else:
    doc.write('#### Properties\n\n')
  print_properties_header(doc, None, ['Name', 'Description'])
  for prop in message_details['properties']:
    print_property_row(doc, proto_contents, proto, proto, message, prop)
  doc.write('\n')

  option_method_name = message
  if 'method-name' in options:
    option_method_name = options['method-name'] + '.' + message
  
  if 'enums' in message_details and message_details['enums']:
    doc.write('#### Enums\n\n')
    for enum in sorted(message_details['enums']):
      print_proto_enum(doc, message_details['enums'][enum], enum, proto,
          {'header-size': '#####',
           'add-method-name': 1,
           'method-name': option_method_name})

  if 'messages' in message_details and message_details['messages']:
    doc.write('#### Messages\n\n')
    for child_message in sorted(message_details['messages']):
      print_proto_message(doc, proto, proto_contents,
          message_details['messages'][child_message], child_message,
          {'header-size': '#####',
           'add-method-name': 1,
           'method-name': option_method_name})

def print_proto_messages(doc, proto_contents, proto, objects_to_print, options):
  if 'messages' in proto_contents[proto] and proto_contents[proto]['messages']:
    for message in sorted(proto_contents[proto]['messages']):
      if ('messages' in objects_to_print[proto] and
          message in objects_to_print[proto]['messages']):
        print_proto_message(doc, proto, proto_contents,
                            proto_contents[proto]['messages'][message],
                            message, options)

def print_proto_enums(doc, proto_contents, proto, objects_to_print, options):
  if 'enums' in proto_contents[proto] and proto_contents[proto]['enums']:
    for enum in sorted(proto_contents[proto]['enums']):
      if ('enums' in objects_to_print[proto] and
          enum in objects_to_print[proto]['enums']):
        print_proto_enum(doc, proto_contents[proto]['enums'][enum], enum,
                         proto, options)

def create_reference_doc(proto_directory, doc_directory, proto_contents,
                         addl_types):
  for proto in proto_contents:
    if 'service' in proto_contents[proto]:
      if (proto_contents[proto]['service']['name'] and
          proto_contents[proto]['service']['name'] == 'Vitess'):
        doc = open(doc_directory + 'VitessApi.md', 'w')

        #if proto_contents[proto]['file_definition']:
        #  print_api_summary(doc, proto_contents[proto]['file_definition'])

        if proto_contents[proto]['service']['methods']:
          print_method_summary(doc, proto_contents,
                               proto_contents[proto]['service']['methods'])

        if proto_contents[proto]['service']['methods']:
          print_method_details(doc, proto_contents, proto,
                               proto_contents[proto]['service']['methods'],
                               addl_types)
        doc.close()
  return

def parse_method_details(line):
  details = re.findall(r'rpc ([^\(]+)\(([^\)]+)\) returns \(([^\)]+)', line)
  if details:
    return {'name': details[0][0],
            'request': details[0][1],
            'response': details[0][2]}
  return {}

def get_enum_struct(comment):
  return {'comment': comment,
          'values': []}

def get_message_struct(comment):
  return {'comment': comment,
          'enums': {},
          'messages': {},
          'properties': []}

def add_property(message, prop_data, prop_type, comment):
  message['properties'].append({'type': prop_type,
                                'name': prop_data[0][2],
                                'position': prop_data[0][3],
                                'status': prop_data[0][0],
                                'comment': comment})
  return message

def build_property_type_list(types, proto_contents, method):
  [op_file, op_method] = get_op_item(proto_contents, method, 'messages')
  if op_method and 'properties' in op_method:
    for prop in op_method['properties']:
      if 'map' in prop['type']:
        map_fields = re.findall(r'map\s*<([^\,]+)\,\s*([^\>]+)', prop['type'])
        if map_fields:
          for x in range(0,2):
            if '.' in map_fields[0][x]:
              types.append(map_fields[0][x])
            elif map_fields[0][x][0].isupper():
              types.append(op_file.replace('.proto', '') + '.' +
                           map_fields[0][x])
      elif '.' in prop['type']:
        types.append(prop['type'])
      elif prop['type'][0].isupper():
        if prop['type'] in proto_contents[op_file]['messages']:
          types.append(op_file.replace('.proto', '') + '.' + prop['type'])
        elif prop['type'] in proto_contents[op_file]['enums']:
          types.append(op_file.replace('.proto', '') + '.' + prop['type'])
        else:
          for message in proto_contents[op_file]['messages']:
            if 'messages' in proto_contents[op_file]['messages'][message]:
              child_messages = (
                  proto_contents[op_file]['messages'][message]['messages'])
              if (prop['type'] in child_messages and
                  'properties' in child_messages[prop['type']]):
                for child_prop in child_messages[prop['type']]['properties']:
                  if '.' in child_prop['type']:
                    types.append(child_prop['type'])
  return types

def main(proto_directory, doc_directory):
  arg_definitions = {}
  commands = {}
  command_groups = {}
  error_counts = {}
  functions = {}

  # Read the .go files in the /vitess/go/vt/vtctl/ directory
  api_file_path = proto_directory
  proto_dirs = next(os.walk(api_file_path))[2]
  proto_lines = {}
  proto_contents = {}
  for path in proto_dirs:
    if not path.endswith('.proto'):
      continue
    api_proto_file = open(proto_directory + path, 'rU')
    proto_lines[path.replace('pb.go', 'proto')] = api_proto_file.readlines()
    api_proto_file.close()

  # parse .proto files
  for path in proto_lines:
    comment = ''
    enum_values = []
    inside_service = ''
    current_message = {}
    current_top_level_message = {}
    current_hierarchy = []
    current_struct = ''
    syntax_specified = False
    proto_contents[path] = {'file_definition': '',
                            'imports': [],
                            'enums': {},
                            'messages': {},
                            'methods': {},
                            'service': {'name': '',
                                        'methods': []}
                           }
    for original_line in proto_lines[path]:
      line = original_line.strip()
      if line[0:8] == 'syntax =':
        syntax_specified = True
        continue
      if line[0:2] == '//' and not syntax_specified:
        proto_contents[path]['file_definition'] += (' ' + line[2:].strip())
        continue
      elif line[0:2] == '//':
        if 'TODO' not in line:
          comment += ' ' + line[2:].strip()
      elif line[0:6] == 'import':
        import_file = line[6:].strip().rstrip(';').strip('"').split('/').pop()
        proto_contents[path]['imports'].append(import_file)
      elif line[0:8] == 'service ':
        service = line[8:].strip().rstrip('{').strip()
        proto_contents[path]['service']['name'] = service
        inside_service = service
        comment = ''
      elif inside_service:
        if line[0:4] == 'rpc ':
          method_details = parse_method_details(line)
          if method_details:
            if comment:
              method_details['comment'] = comment.strip()
            proto_contents[path]['service']['methods'].append(method_details)
            comment = ''

      elif line == '}':
        item_to_add = current_hierarchy.pop().split('-')
        if item_to_add[0] == 'enum':
          current_enum['values'] = enum_values
          enum_values = []
          if len(current_hierarchy) > 0:
            go_back_to_struct = current_hierarchy[-1].split('-')[0]
            if go_back_to_struct == 'topLevelMessage':
              current_top_level_message['enums'][item_to_add[1]] = current_enum
            elif go_back_to_struct == 'message':
              current_message['enums'][item_to_add[1]] = current_enum
            current_struct = go_back_to_struct
          else:
            if current_struct == 'enum':
              proto_contents[path]['enums'][item_to_add[1]] = current_enum
              current_struct = ''
        elif item_to_add[0] == 'message':
          current_top_level_message['messages'][item_to_add[1]] = (
              current_message)
          current_struct = current_hierarchy[-1].split('-')[0]
        elif item_to_add[0] == 'topLevelMessage':
          proto_contents[path]['messages'][item_to_add[1]] = (
              current_top_level_message)
          current_struct = ''
      elif original_line[0:8] == 'message ':
        message = line[8:].strip().rstrip('{').strip()
        current_top_level_message = get_message_struct(comment)
        comment = ''
        current_hierarchy.append('topLevelMessage-' + message)
        current_struct = 'topLevelMessage'
      elif line[0:8] == 'message ':
        message = line[8:].strip().rstrip('{').strip()
        current_message = get_message_struct(comment)
        current_hierarchy.append('message-' + message)
        current_struct = 'message'
      elif line[0:5] == 'enum ':
        enum = line[5:].strip().rstrip('{').strip()
        current_enum = get_enum_struct(comment)
        current_hierarchy.append('enum-' + enum)
        current_struct = 'enum'
        comment = ''
      elif current_struct == 'enum':
        enum_value_data = re.findall(r'([a-zA-Z0-9_]+)\s*=\s*(\d+)', line)
        if enum_value_data:
          enum_values.append({'comment': comment,
                              'text': enum_value_data[0][0],
                              'value': enum_value_data[0][1]})
          comment = ''
        
      else:
        prop_data = re.findall(r'(optional|repeated|required)?\s*([\w\.\_]+)\s+([\w\.\_]+)\s*=\s*(\d+)', line)
        if prop_data:
          if current_struct == 'topLevelMessage':
            current_top_level_message = add_property(current_top_level_message,
                                                     prop_data, prop_data[0][1],
                                                     comment)
          elif current_struct == 'message':
            current_message = add_property(current_message, prop_data,
                                           prop_data[0][1], comment)
          comment = ''
        else:
          prop_data = re.findall(r'(optional|repeated|required)?\s*map\s*\<([^\>]+)\>\s+([\w\.\_]+)\s*=\s*(\d+)', line)
          if prop_data:
            prop_type = 'map <' + prop_data[0][1] + '>' 
            if current_struct == 'topLevelMessage':
              current_top_level_message = add_property(
                  current_top_level_message, prop_data, prop_type, comment)
            elif current_struct == 'message':
              current_message = add_property(current_message, prop_data,
                  prop_type, comment)
            comment = ''

  #print json.dumps(proto_contents, sort_keys=True, indent=2)
  methods = []
  types = []
  for method in proto_contents['vtgateservice.proto']['service']['methods']:
    methods.append(method['request'])
    methods.append(method['response'].replace('stream ', ''))
  for method in methods:
    types = build_property_type_list(types, proto_contents, method)
  types = list(set(types))

  type_length = len(types)
  new_type_length = 100
  for x in range(0, 10):
  #while type_length < new_type_length:
    for prop_type in types:
      types = build_property_type_list(types, proto_contents, prop_type)
      types = list(set(types))
    new_type_length = len(types)
    
  proto_contents['group-ordering'] = ['Range-based Sharding',
                                      'Transactions',
                                      'Custom Sharding',
                                      'Map Reduce',
                                      'Topology',
                                      'v3 API (alpha)']

  create_reference_doc(proto_directory, doc_directory, proto_contents, types)

  return

if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option('-p', '--proto-directory', default='../proto/',
                    help='The root directory for the Vitess GitHub tree')
  parser.add_option('-d', '--doc-directory', default='',
                    help='The directory where the documentation resides.')
  (options,args) = parser.parse_args()
  main(options.proto_directory, options.doc_directory)
