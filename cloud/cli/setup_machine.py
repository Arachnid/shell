"""
Functions for setting up the local machine's API Keys.
This module is only intended for internal use
"""
"""
Copyright (c) 2012 `PiCloud, Inc. <http://www.picloud.com>`_.  All rights reserved.

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
import sys
import getpass

import cloud
from cloud.util import credentials


def setup_machine(email=None, password=None, api_key=None):
    """Prompts user for login information and then sets up api key on the
    local machine
    
    If api_key is a False value, interpretation is:
        None: create api key
        False: Prompt for key selection
    
    """
    
    # Disable simulator -- we need to initiate net connections
    cloud.config.use_simulator = False
    cloud.config.commit()
    
    if not email or not password:
        print 'Please enter your PiCloud account login information.\nIf you do not have an account, please create one at http://www.picloud.com'
    
    try:
        if email:
            print 'E-mail: %s' % email
        else:
            email = raw_input('E-mail: ')
            
        if password:
            print('Password: ')
        else:
            password = getpass.getpass('Password: ')
        
        if api_key is False:
            keys = cloud.account.list_keys(email, password, active_only=True)
            
            print """\nPiCloud uses API Keys, rather than your login information, to authenticate
your machine. In the event your machine is compromised, you can deactivate
your API Key to disable access. In this next step, you can choose to use
an existing API Key for this machine, or create a new one. We recommend that
each machine have its own unique key."""
            
            print '\nYour API Key(s)'
            for key in keys:
                print key
            
            api_key = raw_input('\nPlease select an API Key or just press enter to create a new one automatically: ')
            if api_key:
                key = cloud.account.get_key(email, password, api_key)
            else:
                key = cloud.account.create_key(email, password)
                print 'API Key: %s' % key['api_key']
            
        elif api_key is None or api_key == '':
            key = cloud.account.create_key(email, password)
            print 'API Key: %s' % key['api_key']
        else:
            key = cloud.account.get_key(email, password, api_key)
            print 'API Key: %s' % key['api_key']
        
        # save all key credentials
        credentials.save_keydef(key)
        
        # set config and write it to file
        cloud.config.api_key = key['api_key']
        cloud.config.commit()
        cloud.cloudconfig.flush_config()        
                
        
        # if user is running "picloud setup" with sudo, we need to chown
        # the config file so that it's owned by user and not root.
        sudouid = os.environ.get('SUDO_UID')
        if sudouid:
            confname = cloud.cloudconfig.fullconfigpath + cloud.cloudconfig.configname
            os.chown(confname, int(sudouid),  int(os.environ.get('SUDO_GID')))
            os.chown(cloud.cloudconfig.fullconfigpath, int(sudouid),  int(os.environ.get('SUDO_GID')))
        
        try:
            import platform
            conn = cloud._getcloudnetconnection()
            conn.send_request('report/install/', {'hostname': platform.node(),
                                                  'language_version': platform.python_version(),
                                                  'language_implementation': platform.python_implementation(),
                                                  'platform': platform.platform(),
                                                  'architecture': platform.machine(),
                                                  'processor': platform.processor(),
                                                  })
        except:
            pass
        
    except EOFError:
        sys.stderr.write('Got EOF. Please run "picloud setup" to complete installation.\n')
        sys.exit(1)
    except KeyboardInterrupt:
        sys.stderr.write('Got Keyboard Interrupt. Please run "picloud setup" to complete installation.\n')
        sys.exit(1)
    except cloud.CloudException, e:
        sys.stderr.write(str(e)+'\n')
        sys.exit(3)
    else:
        print '\nSetup successful!'
