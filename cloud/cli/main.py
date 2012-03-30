#!/usr/bin/python
"""
Entry Point for the PiCloud Command-Line Interface (CLI)
"""
# since this module sits in the cloud package, we use absolute_import
# so that we can easily import the top-level package, rather than the
# cloud module inside the cloud package
from __future__ import absolute_import
"""
Copyright (c) 2011 `PiCloud, Inc. <http://www.picloud.com>`_.  All rights reserved.

email: contact@picloud.com

The cloud package is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This package is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this package; if not, see 
http://www.gnu.org/licenses/lgpl-2.1.html    
"""

import sys
import logging
import traceback

try:
    import json
except:
    # Python 2.5 compatibility
    import simplejson as json

import cloud

from . import argparsers
from .util import list_of_dicts_printer, dict_printer, list_printer, NullHandler, volume_ls_printer
from .setup_machine import setup_machine


def main(args=None):
    """Entry point for PiCloud CLI. If *args* is None, it is assumed that main()
    was called from the command-line, and sys.argv is used."""
    
    args = args or sys.argv[1:]
        
    # special case: we want to provide the full help information
    # if the cli is run with no arguments
    if len(args) == 0:
        argparsers.picloud_parser.print_help()
        sys.exit(1)
    
    # special case: if --version is specified at all, print it out
    if '--version' in args:
        print cloud.__version__
        sys.exit(0)
        
    # parse_args is an object whose attributes are populated by the parsed args
    parsed_args = argparsers.picloud_parser.parse_args(args)
    
    cloud.config._showhidden()
    cloud.config.verbose = parsed_args._verbose
    # suppress log messages
    cloud.config.print_log_level = logging.getLevelName(logging.CRITICAL)    
    cloud.config.commit()
    
    module_name = getattr(parsed_args, '_module', '')
    command_name = getattr(parsed_args, '_command', '')
    function_name = module_name + ('.%s' % command_name if command_name else '')
    
    # we take the attributes from the parsed_args object and pass them in
    # as **kwargs to the appropriate function. attributes with underscores
    # are special, and thus we filter them out.
    kwargs = dict([(k, v) for k,v in parsed_args._get_kwargs() if not k.startswith('_')])
    
    # we keep function_mapping and printer_mapping here to prevent
    # circular imports
    
    # maps the output of the parser to what function should be called
    function_mapping = {'setup': setup_machine,
                        'files.get': cloud.files.get,
                        'files.put': cloud.files.put,
                        'files.list': cloud.files.list,
                        'files.delete': cloud.files.delete,
                        'files.get-md5': cloud.files.get_md5,
                        'files.sync-from-cloud': cloud.files.sync_from_cloud,
                        'files.sync-to-cloud': cloud.files.sync_to_cloud,
                        'realtime.request': cloud.realtime.request,
                        'realtime.release': cloud.realtime.release,
                        'realtime.list': cloud.realtime.list,
                        'volume.list': cloud.volume.get_list,
                        'volume.create': cloud.volume.create,
                        'volume.sync': cloud.volume.sync,
                        'volume.delete': cloud.volume.delete,
                        'volume.ls': cloud.volume.ls,
                        'volume.rm': cloud.volume.rm,
                        }
    
    # maps the called function to another function for printing the output
    printer_mapping = {'realtime.request': dict_printer(['request_id', 'type', 'cores', 'start_time']),
                       'realtime.list': list_of_dicts_printer(['request_id', 'type', 'cores', 'start_time']),
                       'files.list': list_printer('filename'),
                       'volume.list': list_of_dicts_printer(['name', 'mnt_path', 'created', 'desc']),
                       'volume.ls': volume_ls_printer,
                       }

    try:
        # execute function
        ret = function_mapping[function_name](**kwargs) 

        if parsed_args._output == 'json':
            print json.dumps(ret)
        else:
            if function_name in printer_mapping:
                # use a printer_mapping if it exists
                # this is how dict/tables and lists with columns are printed
                printer_mapping[function_name](ret, parsed_args._output != 'no-header')
            else:
                if isinstance(ret, (tuple, list)):
                    # if it's just  list with not mapping, just print it
                    for item in ret:
                        print item
                elif ret:
                    # cases where the output is just a string or number
                    print ret
                else:
                    # if the output is None, print nothing
                    pass
            
    except cloud.CloudException, e:
        # error thrown by cloud client library
        sys.stderr.write(str(e)+'\n')
        sys.exit(3)
        
    except Exception, e:
        # unexpected errors
        sys.stderr.write('Got unexpected error\n')
        traceback.print_exc(file = sys.stderr)
        sys.exit(1)
        
    else:
        sys.exit(0)
