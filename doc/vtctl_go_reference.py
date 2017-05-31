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
import re

# TODO: Handle angle brackets that appear in command definitions --
#       e.g. ChangeSlaveType


# Traverse directory to get list of all files in the directory.
def get_all_files(directory, filenames):
  os.chdir(directory)
  for path, dirs, files in os.walk('.'):
    for filename in files:
      filenames[os.path.join(path, filename)] = True
  return filenames

# This needs to produce the same anchor ID as the Markdown processor.
def anchor_id(heading):
  return heading.lower().replace(' ', '-').replace(',', '')

def write_header(doc, commands):
  doc.write('This reference guide explains the commands that the ' +
            '<b>vtctl</b> tool supports. **vtctl** is a command-line tool ' +
            'used to administer a Vitess cluster, and it allows a human ' +
            'or application to easily interact with a Vitess ' +
            'implementation.\n\nCommands are listed in the ' +
            'following groups:\n\n')
  for group in sorted(commands):
    group_link = anchor_id(group)
    doc.write('* [' + group + '](#' + group_link + ')\n')
  doc.write('\n\n')

def write_footer(doc):
  doc.write('  </body>\n')
  doc.write('</html>\n')

def create_reference_doc(root_directory, commands, arg_definitions):
  doc = open(root_directory + '/doc/vtctlReference.md', 'w')
  write_header(doc, commands)

  not_found_arguments = {}
  for group in sorted(commands):
    doc.write('## ' + group + '\n\n');
    for command in sorted(commands[group]):
      if ('definition' in commands[group][command] and
          commands[group][command]['definition'] != '' and
          re.search(r'HIDDEN', commands[group][command]['definition'])):
        print '\n\n****** ' + command + ' is hidden *******\n\n'
        continue
      command_link = anchor_id(command)
      doc.write('* [' + command + '](#' + command_link + ')\n')

    doc.write('\n')

    for command in sorted(commands[group]):
      if ('definition' in commands[group][command] and
          commands[group][command]['definition'] != '' and
          re.search(r'HIDDEN', commands[group][command]['definition'])):
        continue
      doc.write('### ' + command + '\n\n');
      if ('definition' in commands[group][command] and
          commands[group][command]['definition'] != ''):
        commands[group][command]['definition'] = (
            commands[group][command]['definition'].replace('<', '&lt;'))
        commands[group][command]['definition'] = (
            commands[group][command]['definition'].replace('>', '&gt;'))
        commands[group][command]['definition'] = (
            commands[group][command]['definition'].replace('&lt;/a&gt;',
                                                           '</a>'))
        commands[group][command]['definition'] = (
            commands[group][command]['definition'].replace('&lt;a href',
                                                           '<a href'))
        commands[group][command]['definition'] = (
            commands[group][command]['definition'].replace('"&gt;', '">'))
        commands[group][command]['definition'] = (
            commands[group][command]['definition'].replace('"&lt;br&gt;',
                                                           '<br>'))
        commands[group][command]['definition'] = (
            commands[group][command]['definition'].replace('\\n', '<br><br>'))
        doc.write(commands[group][command]['definition'] + '\n\n')

      if ('arguments' in commands[group][command] and 
          commands[group][command]['arguments']):
        doc.write('#### Example\n\n')
        arguments = commands[group][command]['arguments'].strip().strip(
            '"').replace('<', '&lt;')
        arguments = arguments.strip().replace('>', '&gt;')
        doc.write('<pre class="command-example">%s %s</pre>\n\n' %
                 (command, arguments))

      if ('argument_list' in commands[group][command] and
          'flags' in commands[group][command]['argument_list']):
        flag_text = ''
        for command_flag in sorted(
            commands[group][command]['argument_list']['flags']):
          flag = (
              commands[group][command]['argument_list']['flags'][command_flag])
          flag_text += ('| %s | %s | %s |\n' % (command_flag, flag['type'],
                                                flag['definition']))

        if flag_text:
          #flag_text = '<a name="' + command + '-flags"></a>\n\n' +
          flag_text = ('| Name | Type | Definition |\n' +
                       '| :-------- | :--------- | :--------- |\n' +
                       flag_text + '\n')
          doc.write('#### Flags\n\n' + flag_text + '\n')

      if ('argument_list' in commands[group][command] and
          'args' in commands[group][command]['argument_list']):
        arg_text = ''

        for arg in commands[group][command]['argument_list']['args']:
          if 'name' in arg:
            arg_name = arg['name']
            new_arg_name = arg['name'].replace('<', '').replace('>', '')
            if (new_arg_name[0:len(new_arg_name) - 1] in arg_definitions and
                re.search(r'\d', new_arg_name[-1])):
              arg_name = '<' + new_arg_name[0:len(new_arg_name) - 1] + '>'
            arg_name = arg_name.strip().replace('<', 'START_CODE_TAG&lt;')
            arg_name = arg_name.strip().replace('>', '&gt;END_CODE_TAG')
          arg_text += '* ' + arg_name + ' &ndash; '
          if 'required' in arg and arg['required']:
            arg_text += 'Required.'
          else:
            arg_text += 'Optional.'

          arg_values = None
          temp_name = arg['name'].replace('<', '').replace('>', '')
          if temp_name in arg_definitions:
            arg_text += ' ' + arg_definitions[temp_name]['description']
            if 'list_items' in arg_definitions[temp_name]:
              arg_values = arg_definitions[temp_name]['list_items']
          # Check if the argument name ends in a digit to catch things like
          # keyspace1 being used to identify the first in a list of keyspaces.
          elif (temp_name[0:len(temp_name) - 1] in arg_definitions and
                re.search(r'\d', temp_name[-1])):
            arg_length = len(temp_name) - 1
            arg_text += (' ' +
                arg_definitions[temp_name[0:arg_length]]['description'])
            if 'list_items' in arg_definitions[temp_name[0:arg_length]]:
              arg_values = (
                  arg_definitions[temp_name[0:arg_length]]['list_items'])
          else:
            not_found_arguments[arg['name']] = True
          if arg_values:
            arg_text += '\n\n'
            for arg_value in sorted(arg_values, key=lambda k: k['value']):
              arg_text += ('    * <code>' + arg_value['value'] + '</code> ' +
                           '&ndash; ' + arg_value['definition'] + '\n')
            arg_text += '\n\n'

          if 'multiple' in arg:
            separation = 'space'
            if 'hasComma' in arg:
              separation = 'comma'
            arg_text += (' To specify multiple values for this argument, ' +
                         'separate individual values with a ' +
                         separation + '.')
          arg_text += '\n'
        if arg_text:
          arg_text = arg_text.replace('START_CODE_TAG', '<code>')
          arg_text = arg_text.replace('END_CODE_TAG', '</code>')
          doc.write('#### Arguments\n\n' +
                    arg_text +
                    '\n')

      if 'errors' in commands[group][command]:
        errors_text = ''
        if 'ARG_COUNT' in commands[group][command]['errors']:
          error = commands[group][command]['errors']['ARG_COUNT']
          message = re.sub(str(command), '<' + command + '>', error['message'])
          #print 'message here'
          #print message
          #message = error['message'].replace(command, '<' + command + '>')
          message = message.replace('<', 'START_CODE_TAG&lt;')
          message = message.replace('>', '&gt;END_CODE_TAG')
          errors_text += '* ' + message + ' '
          if (error['exact_count'] and
              len(error['exact_count']) == 1 and
              error['exact_count'][0] == '1'):
            errors_text += ('This error occurs if the command is not called ' +
                            'with exactly one argument.')
          elif error['exact_count'] and len(error['exact_count']) == 1:
            errors_text += ('This error occurs if the command is not called ' +
                            'with exactly ' + error['exact_count'][0] + ' ' +
                            'arguments.')
          elif error['exact_count']:
            allowed_error_counts = ' or '.join(error['exact_count'])
            errors_text += ('This error occurs if the command is not called ' +
                            'with exactly ' + allowed_error_counts + ' ' +
                            'arguments.')
          elif error['min_count'] and error['max_count']:
            errors_text += ('This error occurs if the command is not called ' +
                            'with between ' + error['min_count'] + ' and ' +
                            error['max_count'] + ' arguments.')
          elif error['min_count'] and error['min_count'] == '1':
            errors_text += ('This error occurs if the command is not called ' +
                            'with at least one argument.')
          elif error['min_count']:
            errors_text += ('This error occurs if the command is not called ' +
                            'with at least ' + error['min_count'] + ' ' +
                            'arguments.')
          elif error['max_count']:
            errors_text += ('This error occurs if the command is not called ' +
                            'with more than ' + error['max_count'] + ' ' +
                            'arguments.')
          errors_text += '\n'

        if 'other' in commands[group][command]['errors']:
          #print 'other errors'
          #print commands[group][command]['errors']
          for error in commands[group][command]['errors']['other']:
            error = re.sub(str(command), '<' + command + '>', error)
            if ('argument_list' in commands[group][command] and
                'flags' in commands[group][command]['argument_list']):
              for command_flag in sorted(
                  commands[group][command]['argument_list']['flags']):
                error = re.sub(str(command_flag), '<' + command_flag + '>',
                               error)

            error = error.replace('<', 'START_CODE_TAG&lt;')
            error = error.replace('>', '&gt;END_CODE_TAG')
            errors_text += '* ' + error + '\n'

        if errors_text:
          errors_text = errors_text.replace('START_CODE_TAG', '<code>')
          errors_text = errors_text.replace('END_CODE_TAG', '</code>')
          doc.write('#### Errors\n\n' + errors_text)
      doc.write('\n\n')
  #write_footer(doc)
  doc.close()
  #print json.dumps(not_found_arguments, sort_keys=True, indent=4)
  return

def parse_arg_list(arguments, current_command):
  last_char = ''

  find_closing_square_bracket =  False
  has_comma = False
  has_multiple = False
  is_optional_argument = False
  is_required_argument = False
  current_argument = ''

  new_arg_list = []
  arg_count = 0
  char_count = 1
  for char in arguments:
    if (last_char == '' or last_char == ' ') and char == '[':
      find_closing_square_bracket =  True
    elif (last_char == '[' and
          find_closing_square_bracket and
          char == '<'):
      is_optional_argument = True
      current_argument += char
    elif find_closing_square_bracket:
      if char == ']':
        if is_optional_argument:
          new_arg_list.append({'name': current_argument,
                               'has_comma': has_comma,
                               'has_multiple': has_multiple,
                               'required': False})
          arg_count += 1
        find_closing_square_bracket =  False
        current_argument = ''
        has_comma = False
        has_multiple = False
      elif is_optional_argument:
        if char == ',':
          has_comma = True
        elif last_char == '.' and char == '.':
          has_multiple = True
        elif not has_comma:
          current_argument += char
    elif char == '<' and (last_char == '' or last_char == ' '):
      is_required_argument = True
      current_argument += char
    elif char == ',':
      has_comma = True
      if last_char == '>':
        new_arg_list[arg_count - 1]['hasComma'] = True
      has_comma = False
    elif is_required_argument:
      current_argument += char
      if char == '>' and current_argument:
        if char_count == len(arguments[0]):
          new_arg_list.append({'name': current_argument,
                               'required': True})
          arg_count += 1
          is_required_argument = False
          current_argument = ''
        else:
          next_char = 'x'
          if current_command == 'Resolve':
            if char_count < len(arguments[0]):
              next_char = arguments[0][char_count:char_count + 1]

          if next_char and not next_char == '.' and not next_char == ':':
            new_arg_list.append({'name': current_argument,
                                 'required': True})
            arg_count += 1
            is_required_argument = False
            current_argument = ''
    elif (arg_count > 0 and
          current_argument == '' and
          last_char == '.' and
          'multiple' in new_arg_list[arg_count - 1] and
          char == ' '):
        current_argument = ''
    elif (arg_count > 0 and
          current_argument == '' and
          last_char == '.' and
          char == '.'):
      new_arg_list[arg_count - 1]['multiple'] = True

    char_count += 1
    last_char = char
  return new_arg_list

def get_group_name_from_variable(file_path, variable_name):
  vtctl_go_file = open(file_path, 'rU')
  vtctl_go_data = vtctl_go_file.readlines()
  vtctl_go_file.close()

  for line in vtctl_go_data:
    regex = r'const\s*' + re.escape(variable_name) + r'\s*=\s*\"([^\"]+)\"'
    if re.search(regex, line):
      return line.split('=')[1].strip().strip('"')
  return variable_name

def main(root_directory):
  arg_definitions = {}
  commands = {}
  command_groups = {}
  error_counts = {}
  functions = {}

  # Read the .go files in the /vitess/go/vt/vtctl/ directory
  vtctl_dir_path = root_directory + '/go/vt/vtctl/'
  go_files = next(os.walk(vtctl_dir_path))[2]
  for path in go_files:
    if not path.endswith('.go'):
      continue
    vtctl_go_path = vtctl_dir_path + path
    vtctl_go_file = open(vtctl_go_path, 'rU')
    vtctl_go_data = vtctl_go_file.readlines()
    vtctl_go_file.close()

    add_command_syntax = False
    get_commands = False
    get_argument_definitions = False
    get_wrong_arg_count_error = False
    get_group_name = False
    current_command_argument = ''
    current_command_argument_value = ''
    current_command = ''
    current_function = ''
    is_func_init = False
    is_flag_section = False

    # treat func init() same as var commands
    # treat addCommand("Group Name"... same as command {... in vtctl.go group
    # Reformat Generic Help command to same format as commands in backup.go
    # and reparent.go.
    # Add logic to capture command data from those commands.
    for line in vtctl_go_data:

      # skip comments and empty lines
      if line.strip() == '' or line.strip().startswith('//'):
        continue

      if is_func_init and not is_flag_section and re.search(r'^if .+ {', line.strip()):
          is_flag_section = True
      elif is_func_init and not is_flag_section and line.strip() == 'servenv.OnRun(func() {':
        pass
      elif is_func_init and is_flag_section and line.strip() == 'return':
        pass
      elif is_func_init and is_flag_section and line.strip() == '}':
        is_flag_section = False
      elif is_func_init and (line.strip() == '}' or line.strip() == '})'):
        is_func_init = False
      elif get_commands:
        # This line precedes a command group's name, e.g. "Tablets" or "Shards."
        # Capture the group name on the next line.
        if line.strip() == '{':
          get_group_name = True
        # Capture the name of a command group.
        elif get_group_name:
          # Regex to identify the group name. Line in code looks like:
          #   "Tablets", []command{
          find_group = re.findall(r'\"([^\"]+)\", \[\]\s*command\s*\{', line)
          if find_group:
            current_group = find_group[0]
            if current_group not in commands:
              commands[current_group] = {}
            get_group_name = False

        # First line of a command in addCommand syntax. This contains the
        # name of the group that the command is in. Line in code looks like:
        #   addCommand{"Shards", command{
        elif re.search(r'^addCommand\(', line.strip()):
          command_data = re.findall(r'addCommand\s*\(\s*([^\,]+),\s*command\{',
                                    line)
          if command_data:
            current_group = command_data[0]
            current_group_strip_quotes = current_group.strip('"')
            if current_group == current_group_strip_quotes:
              current_group = get_group_name_from_variable(vtctl_go_path,
                                                           current_group)
            else:
              current_group = current_group_strip_quotes
            if current_group not in commands:
              commands[current_group] = {}
            add_command_syntax = True

        elif add_command_syntax and is_func_init:
          if not current_command:
            current_command = line.strip().strip(',').strip('"')
            commands[current_group][current_command] = {
                'definition': '',
                'argument_list': {
                                  'flags': {},
                                  'args': []
                                 },
                'errors': {'other': []}}
            command_groups[current_command] = current_group
          elif 'function' not in commands[current_group][current_command]:
            function = line.strip().strip(',')
            commands[current_group][current_command]['function'] = function
            functions[function] = current_command
          elif 'arguments' not in commands[current_group][current_command]:
            arguments = line.strip().strip(',')
            commands[current_group][current_command]['arguments'] = arguments
            if arguments:
              new_arg_list = parse_arg_list(arguments, current_command)
              commands[current_group][current_command]['argument_list']['args'] = new_arg_list
          else:
            definition_list = line.strip().split(' +')
            for definition_part in definition_list:
              definition = definition_part.strip().strip('})')
              definition = definition.replace('}},', '')
              definition = definition.replace('"', '')
              commands[current_group][current_command]['definition'] += (
                  definition)
            if line.strip().endswith('})'):
              current_command = ''
        # Command definition ends with line ending in "},".
        elif line.strip().endswith('})'):
          current_command = ''
          add_command_syntax = False

        # First line of a command. This contains the command name and the
        # function used to process the command. Line in code looks like:
        #   command{"ScrapTablet", commandScrapTablet,
        elif re.search(r'^\{\s*\"[^\"]+\",\s*command[^\,]+\,', line.strip()):
          command_data = re.findall(r'\{\s*\"([^\"]+)\",\s*([^\,]+)\,',
                                    line)
          if command_data:
            # Capture the command name and associate it with its function.
            # Create a data structure to contain information about the command
            # and its processing rules.
            current_command = command_data[0][0]
            function = command_data[0][1]
            commands[current_group][current_command] = {
                'definition': '',
                'argument_list': {
                                  'flags': {},
                                  'args': []
                                 },
                'errors': {'other': []}}

            # Associate the function with the command and vice versa.
            # Associate the command with its group and vice versa.
            commands[current_group][current_command]['function'] = function
            command_groups[current_command] = current_group
            functions[function] = current_command

        # If code has identified a command name but has not identified
        # arguments for that command, capture the next line and store it
        # as the command arguments.
        elif (current_command and
              'arguments' not in commands[current_group][current_command]):
          arguments = re.findall(r'\"([^\"]+)\"', line)
          if arguments:
            commands[current_group][current_command]['arguments'] = arguments[0]
            last_char = ''

            find_closing_square_bracket =  False
            has_comma = False
            has_multiple = False
            is_optional_argument = False
            is_required_argument = False
            current_argument = ''

            new_arg_list = []
            arg_count = 0
            char_count = 1
            for char in arguments[0]:
              if (last_char == '' or last_char == ' ') and char == '[':
                find_closing_square_bracket =  True
              elif (last_char == '[' and
                    find_closing_square_bracket and
                    char == '<'):
                is_optional_argument = True
                current_argument += char
              elif find_closing_square_bracket:
                if char == ']':
                  if is_optional_argument:
                    new_arg_list.append({'name': current_argument,
                                         'has_comma': has_comma,
                                         'has_multiple': has_multiple,
                                         'required': False})
                    arg_count += 1
                  find_closing_square_bracket =  False
                  current_argument = ''
                  has_comma = False
                  has_multiple = False
                elif is_optional_argument:
                  if char == ',':
                    has_comma = True
                  elif last_char == '.' and char == '.':
                    has_multiple = True
                  elif not has_comma:
                    current_argument += char
              elif char == '<' and (last_char == '' or last_char == ' '):
                is_required_argument = True
                current_argument += char
              elif char == ',':
                has_comma = True
                if last_char == '>':
                  new_arg_list[arg_count - 1]['hasComma'] = True
                has_comma = False
              elif is_required_argument:
                current_argument += char
                if char == '>' and current_argument:
                  if char_count == len(arguments[0]):
                    new_arg_list.append({'name': current_argument,
                                         'required': True})
                    arg_count += 1
                    is_required_argument = False
                    current_argument = ''
                  else:
                    next_char = 'x'
                    if current_command == 'Resolve':
                      #print len(arguments[0])
                      #print char_count
                      if char_count < len(arguments[0]):
                        next_char = arguments[0][char_count:char_count + 1]
                        #print next_char

                    #if char_count < (len(arguments[0]) - 1):
                    #  print current_argument
                    #  next_char = arguments[0][char_count + 1:char_count + 2]
                    #  print next_char
                    if next_char and not next_char == '.' and not next_char == ':':
                      new_arg_list.append({'name': current_argument,
                                           'required': True})
                      arg_count += 1
                      is_required_argument = False
                      current_argument = ''
              elif (arg_count > 0 and
                    current_argument == '' and
                    last_char == '.' and
                    'multiple' in new_arg_list[arg_count - 1] and
                    char == ' '):
                  current_argument = ''
              elif (arg_count > 0 and
                    current_argument == '' and
                    last_char == '.' and
                    char == '.'):
                new_arg_list[arg_count - 1]['multiple'] = True
              #elif (arg_count > 0 and
              #      current_argument == '' and
              #      last_char == '.'):

              char_count += 1
              last_char = char
            commands[current_group][current_command]['argument_list']['args'] = (
                new_arg_list)

          else:
            commands[current_group][current_command]['arguments'] = ""
        # If code has identified a command and arguments, capture remaining lines
        # as the command description. Assume the description ends at the line
        # of code ending with "},".
        elif current_command:
          definition_list = line.rstrip('},').split(' +')
          for definition_part in definition_list:
            definition = definition_part.strip().strip('"')
            #definition = definition.replace('\\n', '<br><br>')
            definition = definition.replace('"},', '')
            commands[current_group][current_command]['definition'] += definition
          if line.strip().endswith('},'):
            current_command = ''
        # Command definition ends with line ending in "},".
        elif line.strip().endswith('},'):
          current_command = ''

        # Capture information about a function that processes a command.
        # Here, identify the function name.
        elif re.search(r'^func ([^\)]+)\(', line.strip()):
          current_function = ''
          function_data = re.findall(r'func ([^\)]+)\(', line.strip())
          if function_data:
            current_function = function_data[0]

        elif current_function:
          # Lines that contain this:
          #   = subFlags....
          # generally seem to contain descriptions of flags for the function.
          # Capture the content type of the argument, its name, default value,
          # and description. Associate these with the command that calls this
          # function.
          if re.search(r'\=\s*subFlags\.([^\(]+)\(\s*\"([^\"]+)\"\,' +
                        '([^\,]+)\,\s*\"([^\"]+)\"', line):
            argument_data = re.findall(r'\=\s*subFlags\.([^\(]+)' +
                '\(\s*\"([^\"]+)\"\,([^\,]+)\,\s*\"([^\"]+)\"', line)
            if argument_data and current_function in functions:
              [arg_type, arg_name, arg_default, arg_definition] = argument_data[0]
              if arg_type == 'Bool':
                arg_type = 'Boolean'
              if arg_type == 'String' or arg_type == 'int':
                arg_type = arg_type.lower()

              arg_default = arg_default.strip().strip('"')

              fcommand = functions[current_function]
              fgroup = command_groups[fcommand]
              commands[fgroup][fcommand]['argument_list']['flags'][arg_name] = {
                'type': arg_type,
                'default': arg_default,
                'definition': arg_definition
              }

          elif re.search(r'\s*subFlags\.Var\(\s*([^\,]+)\,\s*\"([^\"]+)\"\,' +
                        '\s*\"([^\"]+)\"', line):
            #print 'found subFlags.Var'
            argument_data = re.findall(r'\s*subFlags\.Var\(\s*([^\,]+)\,' +
                '\s*\"([^\"]+)\"\,\s*\"([^\"]+)\"', line)
            #print argument_data
            if argument_data and current_function in functions:
              [arg_type, arg_name, arg_definition] = argument_data[0]
              arg_type = 'string' # Var?

              fcommand = functions[current_function]
              fgroup = command_groups[fcommand]
              commands[fgroup][fcommand]['argument_list']['flags'][arg_name] = {
                'type': arg_type,
                'definition': arg_definition
              }

          # Capture information for errors that indicate that the command
          # was called with the incorrect number of arguments. Use the
          # code to determine whether the code is looking for an exact number
          # of arguments, a minimum number, a maximum number, etc.
          elif re.search(r'if subFlags.NArg', line):
            wrong_arg_data = re.findall(
                r'subFlags.NArg\(\)\s*([\<\>\!\=]+)\s*(\d+)', line)
            error_counts[current_function] = {'min': None,
                                              'max': None,
                                              'exact': []}
            if wrong_arg_data:
              get_wrong_arg_count_error = True
              for wrong_arg_info in wrong_arg_data:
                if wrong_arg_info[0] == '!=':
                  error_counts[current_function]['exact'].append(
                      wrong_arg_info[1])
                elif wrong_arg_info[0] == '<':
                  error_counts[current_function]['min'] = wrong_arg_info[1]
                elif wrong_arg_info[0] == '>':
                  error_counts[current_function]['max'] = wrong_arg_info[1]
                elif wrong_arg_info[0] == '==' and wrong_arg_info[1] == '0':
                  error_counts[current_function]['min'] = '1'

          # Capture data about other errors that the command might yield.
          # TODO: Capture other errors from other files, such as
          #       //depot/google3/third_party/golang/vitess/go/vt/topo/tablet.go
          elif get_wrong_arg_count_error and re.search(r'fmt.Errorf', line):
            get_wrong_arg_count_error = False
            error_data = re.findall(r'fmt\.Errorf\(\"([^\"]+)\"\)', line)
            if error_data and current_function in functions:
              fcommand = functions[current_function]
              fgroup = command_groups[fcommand]
              commands[fgroup][fcommand]['errors']['ARG_COUNT'] = {
                'exact_count': error_counts[current_function]['exact'],
                'min_count': error_counts[current_function]['min'],
                'max_count': error_counts[current_function]['max'],
                'message': error_data[0]
              }
          elif line.strip().endswith('}') or line.strip().endswith('{'):
            get_wrong_arg_count_error = False
          elif current_function in functions and re.search(r'fmt.Errorf', line):
            error_data = re.findall(r'fmt\.Errorf\(\"([^\"]+)\"', line)
            if error_data:
              fcommand = functions[current_function]
              fgroup = command_groups[fcommand]
              if (error_data[0] not in
                  commands[fgroup][fcommand]['errors']['other']):
                if ('ARG_COUNT' in commands[fgroup][fcommand]['errors'] and
                    error_data[0] !=
                    commands[fgroup][fcommand]['errors']['ARG_COUNT']['message']):
                  commands[fgroup][fcommand]['errors']['other'].append(
                      error_data[0])
                elif 'ARG_COUNT' not in commands[fgroup][fcommand]['errors']:
                  commands[fgroup][fcommand]['errors']['other'].append(
                      error_data[0])


      # This line indicates that commands are starting. No need to capture
      # stuff before here.
      elif re.search(r'^var commands =', line.strip()):
        get_commands = True

      elif re.search(r'^func init\(\)', line.strip()):
        get_commands = True
        is_func_init = True

      if get_argument_definitions:
        if line.strip() == '*/':
          get_argument_definitions = False
        elif line.startswith('-'):
          definition_data = re.findall(r'^\-\s*([^\:]+)\:\s*(.*)', line)
          if definition_data:
            arg_name_array = definition_data[0][0].split('(internal)')
            current_command_argument = arg_name_array[0].strip()
            arg_definitions[current_command_argument] = {}
            if len(arg_name_array) > 1:
              arg_definitions[current_command_argument]['internal'] = True
            arg_definitions[current_command_argument]['description'] = (
                definition_data[0][1].strip())
          current_command_argument_value = ''
        elif current_command_argument and line.lstrip()[0:2] == '--':
          if 'list_items' not in arg_definitions[current_command_argument]:
            arg_definitions[current_command_argument]['list_items'] = []
          arg_value_data = re.findall(r'^\--\s*([^\:]+)\:\s*(.*)', line.strip())
          if arg_value_data:
            current_command_argument_value = arg_value_data[0][0]
            argument_definition = arg_value_data[0][1]
          arg_definitions[current_command_argument]['list_items'].append({
              'value': current_command_argument_value,
              'definition': argument_definition})
        elif current_command_argument_value:
            list_length = (len(
                arg_definitions[current_command_argument]['list_items']) - 1)
            arg_definitions[current_command_argument][
                'list_items'][list_length]['definition'] += ' ' + line.strip()
        elif line and current_command_argument:
          arg_definitions[current_command_argument]['description'] += ' ' + line.strip()

      elif re.search(r'^COMMAND ARGUMENT DEFINITIONS', line.strip()):
        get_argument_definitions = True

  # Handle arguments that have different names but same meaning
  new_arg_definitions = {}
  modifiers = ['destination', 'new master', 'original', 'parent', 'served',
               'source', 'target']

  for defined_argument in arg_definitions:
    argument_list = defined_argument.split(',')
    if len(argument_list) > 1:
      for argument_list_item in argument_list:
        new_arg_definitions[argument_list_item.strip()] = (
            arg_definitions[defined_argument])
        for modifier in modifiers:
          new_arg_definitions[modifier + ' ' + argument_list_item.strip()] = (
            arg_definitions[defined_argument])
    else:
      new_arg_definitions[defined_argument] = arg_definitions[defined_argument]
      for modifier in modifiers:
        new_arg_definitions[modifier + ' ' + defined_argument] = (
          arg_definitions[defined_argument])

  #print json.dumps(new_arg_definitions, sort_keys=True, indent=4)
  #print json.dumps(commands["Generic"], sort_keys=True, indent=4)

  create_reference_doc(root_directory, commands, new_arg_definitions)

  return

if __name__ == '__main__':

  parser = optparse.OptionParser()
  parser.add_option('-r', '--root-directory', default='..',
                    help='root directory for the vitess github tree')
  (options, args) = parser.parse_args()
  main(options.root_directory)
