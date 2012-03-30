"""
ArgumentParsers for processing input to the PiCloud CLI.
"""
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

import os

try:
    import argparse
except:
    from . import argparse


def post_op(string):
    def fix_path(name, **kwargs):
        if os.path.exists(string) and os.path.isdir(string):
            return os.path.join(string, name)
        else:
            return string
    return fix_path


"""Primary Parser"""
picloud_parser = argparse.ArgumentParser(prog='picloud', description='PiCloud Command-Line Interface (CLI)')
picloud_parser.add_argument('--version', dest='_version', action='store_true', help='Print version')
picloud_parser.add_argument('-v', '--verbose', dest='_verbose', action='store_true', help='Increase amount of information you are given during command execution')
picloud_parser.add_argument('-o', '--output', dest='_output', default='default', choices=['json', 'no-header'], help='Format of output')
picloud_parser.add_argument('-a', '--api-key', dest='_api_key', help='API key to use')
picloud_parser.add_argument('-k', '--api-secret-key', dest='_api_secret_key', help='API secret key that matches the API key')

picloud_subparsers = picloud_parser.add_subparsers(title='modules',
                                                   #description='valid modules',
                                                   #help='module help',
                                                   dest='_module',
                                                   )

"""Setup Parser"""
setup_parser = picloud_subparsers.add_parser('setup', description='Sets up the current machine to use PiCloud.')
setup_parser.add_argument('--email', '-e', help='Email used to login to your PiCloud account')
setup_parser.add_argument('--password', '-p', help='Password used to login to your PiCloud account')
setup_parser.add_argument('--api-key', '-a', dest='api_key', nargs='?', default=False, help='API Key to use. If specified without a value, a new API Key will be created without prompting')

"""Execute Parser"""
setup_parser = picloud_subparsers.add_parser('exec', description='Executes a program on PiCloud..')
#setup_parser.add_argument('--email', '-e', help='Email used to login to your PiCloud account')
#setup_parser.add_argument('--password', '-p', help='Password used to login to your PiCloud account')
#setup_parser.add_argument('--api-key', '-a', dest='api_key', nargs='?', default=False, help='API Key to use. If specified without a value, a new API Key will be created without prompting')

"""Files Parser"""
files_parser = picloud_subparsers.add_parser('files', description='Module for managing files stored on PiCloud')
files_subparsers = files_parser.add_subparsers(title='commands', dest='_command', help='command help')

files_delete_parser = files_subparsers.add_parser('delete', help='Delete a file stored on PiCloud')
files_delete_parser.add_argument('name', default=None, help='Name of file stored on PiCloud')

files_get_parser = files_subparsers.add_parser('get', help='Retrieve a file from PiCloud')
files_get_parser.add_argument('name', help='Name of file in storage')
files_get_parser.add_argument('destination', type=post_op, help='Local path to save file to')
#files_get_parser.add_argument('--start-byte', help='Starting byte')
#files_get_parser.add_argument('--end-byte', help='Ending byte')

files_getmd5_parser = files_subparsers.add_parser('get-md5', help='Get the md5 checksum of a file stored on PiCloud')
files_getmd5_parser.add_argument('name', default=None, help='Name of file stored on PiCloud')

files_list_parser = files_subparsers.add_parser('list', help='List files in PiCloud Storage')

files_put_parser = files_subparsers.add_parser('put', help='Store a file on PiCloud')
files_put_parser.add_argument('source', help='Local path to file')
files_put_parser.add_argument('name', default=None, help='Name for file specified by source when stored on PiCloud')

files_syncfromcloud_parser = files_subparsers.add_parser('sync-from-cloud', help='Download file if it does not exist locally or has changed')
files_syncfromcloud_parser.add_argument('name', default=None, help='Name of file stored on PiCloud')
files_syncfromcloud_parser.add_argument('destination', type=post_op, help='Local path to save file to')

files_synctocloud_parser = files_subparsers.add_parser('sync-to-cloud', help='Upload file only if it does not exist on PiCloud or has changed')
files_synctocloud_parser.add_argument('source', default=None, help='local path to file')
files_synctocloud_parser.add_argument('name', default=None, help='Name for file specified by source when stored on PiCloud')

"""Realtime Parser"""
realtime_parser = picloud_subparsers.add_parser('realtime', description='Module for managing realtime cores.')
realtime_subparsers = realtime_parser.add_subparsers(title='commands', dest='_command', help='command help')

realtime_list_parser = realtime_subparsers.add_parser('list', help='List realtime reservations')

realtime_release_parser = realtime_subparsers.add_parser('release', help='Release realtime cores')
realtime_release_parser.add_argument('request_id')

realtime_request_parser = realtime_subparsers.add_parser('request', help='Request realtime cores')
realtime_request_parser.add_argument('type', choices=['c1', 'c2', 'm1', 's1'], help='The type of core to reserve.')
realtime_request_parser.add_argument('cores', type=int, help='The number of cores to reserve.')

""" Volume Parser """
volume_parser = picloud_subparsers.add_parser('volume', description='Module for managing volumes stored on PiCloud')
volume_subparsers = volume_parser.add_subparsers(title='commands', dest='_command', help='command help')

volume_list_parser = volume_subparsers.add_parser('list', help='List existing volumes', description='Lists existing cloud volumes')
volume_list_parser.add_argument('-n', '--name', nargs='+', help='Name(s) of volume to list')
volume_list_parser.add_argument('-d', '--desc', action='store_true', default=False, help='Print volume description')

volume_create_parser = volume_subparsers.add_parser('create', help='Create a volume on PiCloud')
volume_create_parser.add_argument('name', help='Name of the volume to create (max 64 chars)')
volume_create_parser.add_argument('mount_path', help='Mount point (absolute path) where jobs should expect this volume.')
volume_create_parser.add_argument('-d', '--desc', default=None, help='Description of the volume (max 1024 chars)')

volume_sync_parser = volume_subparsers.add_parser('sync', help='Sync local directory to volume on PiCloud', description='Syncs a local path and a cloud volume.', formatter_class=argparse.RawDescriptionHelpFormatter,)
volume_sync_parser.add_argument('source', nargs='+', help='Source path that should be synced')
volume_sync_parser.add_argument('dest', help='Destination path that should be synced')

volume_delete_parser = volume_subparsers.add_parser('delete', help='Delete a cloud volume')
volume_delete_parser.add_argument('name', help='Name of the cloud volume to delete')

volume_ls_parser = volume_subparsers.add_parser('ls', help='List the contents of a cloud volume [path]')
volume_ls_parser.add_argument('volume_path', nargs='+', help='Cloud volume path whose contents should be shown')
volume_ls_parser.add_argument('-l', '--extended_info', action='store_true', default=False, help='Use long listing format')

volume_rm_parser = volume_subparsers.add_parser('rm', help='Remove contents from a cloud volume')
volume_rm_parser.add_argument('volume_path', nargs='+', help='Cloud volume path whose contents should be removed')
volume_rm_parser.add_argument('-r', '--recursive', action='store_true', default=False, help='Remove directories and their contents recursively')

