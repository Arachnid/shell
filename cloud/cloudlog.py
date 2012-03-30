"""
Cloudlog controls the logging of all cloud related messages

Copyright (c) 2009 `PiCloud, Inc. <http://www.picloud.com>`_.  All rights reserved.

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

from . import cloudconfig as cc

c = \
"""Filename where cloud log messages should be written.
This path is relative to ~/.picloud/"""
logFilename = cc.logging_configurable('log_filename',
                                     default='cloud.log',  #NOTE: will not create directories
                                     comment =c)
c = \
"""Should log_filename (default of cloud.log) be written with cloud log messages?
Note that cloud will always emit logging messages; this option controls if cloud should have its own log."""
saveLog = cc.logging_configurable('save_log',
                                 default=True,
                                 comment=c)
c = \
"""logging level for cloud messages.
This affects both messages saved to the cloud log file and sent through the python logging system.
See http://docs.python.org/library/logging.html for more information"""
logLevel = cc.logging_configurable('log_level',
                                  default=logging.getLevelName(logging.DEBUG),
                                  comment=c)

c = \
"""logging level for printing cloud log messages to console.
Must be equal or higher than log_level"""
printLogLevel = cc.logging_configurable('print_log_level',
                                  default=logging.getLevelName(logging.ERROR),
                                  comment=c)

datefmt = '%a %b %d %H:%M:%S %Y'

class NullHandler(logging.Handler):
    """A handler that does nothing"""
    def emit(self, record):
        pass


"""Initialize logging"""
def _init_logging():    
    import os, errno
    from warnings import warn
    
    mylog = logging.getLogger("Cloud")    
    
    #clear handlers if any exist
    handlers = mylog.handlers[:]
    for handler in handlers:
        mylog.removeHandler(handler)
        handler.close()
    
    clog_import_error = None
    if saveLog:        
        try:                  
            from .util.cloghandler.cloghandler import ConcurrentRotatingFileHandler as RotatingHandler
        except ImportError, ie:
            #win32 systems
            from logging.handlers import RotatingFileHandler as RotatingHandler
            clog_import_error = ie
            
        path = os.path.expanduser(cc.baselocation)
        try:
            os.makedirs(path)
        except OSError, e: #allowed to exist already
            if e.errno != errno.EEXIST:                
                warn('PiCloud cannot create directory %s. Error is %s' % (path, e))
            
        path += logFilename
        
        try:
            handler = RotatingHandler(path,maxBytes=524288,backupCount=7)
        except Exception, e: #warn on any exception            
            warn('PiCloud cannot open logfile at %s.  Error is %s' % (path, e))
            handler = NullHandler()
        else:
            #hack for SUDO user
            sudouid = os.environ.get('SUDO_UID')
            if sudouid:
                try:
                    os.chown(path, int(sudouid),  int(os.environ.get('SUDO_GID')))
                    os.chown(path.replace(".log","") + ".lock",int(sudouid),  int(os.environ.get('SUDO_GID')))
                except Exception, e: #warn on any exception
                    warn('PiCloud cannot fix SUDO paths.  Error is %s' % (e))
    else:
        #need a null hander
        handler = NullHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(name)s: %(message)s", datefmt =datefmt))
    mylog.addHandler(handler)
    mylog.setLevel(logging.getLevelName(logLevel))    
    
    #start console logging:
    printhandler = logging.StreamHandler()
    printhandler.setLevel(logging.getLevelName(printLogLevel))
    printhandler.setFormatter(
       logging.Formatter("[%(asctime)s] - [%(levelname)s] - %(name)s: %(message)s",
       datefmt= datefmt))
    mylog.addHandler(printhandler)
    
    if not isinstance(handler, NullHandler):
        mylog.debug("Log file (%s) opened" % handler.baseFilename)

        if clog_import_error:
            mylog.warning('Could not use ConcurrentRotatingFileHandler due to import error: %s.\n' +
                          'Likely missing pywin32 packages. Falling back to regular logging; you may experience log corruption if you run multiple python interpreters',
                          clog_import_error)

            
    return mylog
                                                              
cloudLog = _init_logging()


"""verbose mode
Whether the below functions are sent
"""
verbose = cc.logging_configurable('verbose',
                                     default=False, 
                                     comment = "Should cloud library print informative messages to stdout and stderr",
                                     hidden = True)

def stdout(s, auto_newline=True):
    """Write to stdout if verbose
    If auto_newline, add \n to end (like print)"""
    if not verbose:
        return 
    sys.stdout.write(s)
    if auto_newline:
        sys.stdout.write('\n')
    sys.stdout.flush()
               
def stderr(s, auto_newline=True):
    """Write to stderr if verbose
    If auto_newline, add \n to end (like print)"""
    if not verbose:
        return 
    sys.stderr.write(s)
    if auto_newline:
        sys.stderr.write('\n')
    sys.stderr.flush()
