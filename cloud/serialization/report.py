from __future__ import with_statement 
"""
This module is responsible for managing and writing serialization reports

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

import os, datetime, stat, time, threading    
import shutil

import distutils
import distutils.dir_util

from . import pickledebug
from .serializationhandlers import DebugSerializer
from .. import cloudconfig as cc
from ..cloudlog import cloudLog
from pickledebug import DebugPicklingError 

class SerializationReport():
    c = """Path to save object serialization meta-data.
    This path is relative to ~/.picloud/"""    
    serializeLoggingPath = \
        cc.logging_configurable('serialize_logging_path',
                                          default='datalogs/',
                                          comment=c)
    
    c = """Number of days to wait until purging old serialization log directories"""
    purgeDays = cc.logging_configurable('purge_days',
                                        default=7,
                                        comment =c)
    
    #k = __import__('f')
    #p = __builtins__.__import__('g')
    
    pid = None #process identifier
    cntLock = None
        
    def __init__(self, subdir = ""):
        """
        Create logging directory with proper path if subdir is set
        """
        if subdir:     
            logpath = os.path.expanduser("".join([cc.baselocation,self.serializeLoggingPath,subdir,'/']))
            
            self.purge_old_logs(logpath)
            
            #uses pidgin's log path format        
            date = str(datetime.datetime.today().date())
            date = date.replace(':','-')
            
            time = str(datetime.datetime.today().time())[:8]
            time = time.replace(':','')
            
            timestamp = date + '.' + time
            
            logpath += timestamp
                        
            
            try_limit = 10000
            ctr = 0
            basepath = logpath
            
            
            
            while True:
                try:
                    logpath += '/'
                    if not distutils.dir_util.mkpath(logpath):
                        raise distutils.errors.DistutilsFileError('retry')
                except distutils.errors.DistutilsFileError, e:
                    if ctr >= try_limit:
                        raise IOError("can't make file %s. Error is %s" % (logpath,str(e)))
                    ctr+=1                    
                    logpath = basepath + '-%d' % ctr  
                else:
                    break
            
            cloudLog.info("Serialization reports will be written to %s " % logpath)            
            #hack for SUDO users
            sudouid = os.environ.get('SUDO_UID')
            if sudouid:
                os.chown(logpath, int(sudouid),  int(os.environ.get('SUDO_GID')))
                
            self.logPath = logpath
                    
        self.pickleCount = {}
        self.cntLock = threading.Lock()
        
    def purge_old_logs(self, logpath):
        """Remove subdirectories with modified time older than self.purgeDays days"""
        try:
            subdirs = os.listdir(logpath)
        except OSError, e:
            cloudLog.debug('Could not purge %s due to %s', logpath, str(e))
            return
        now = time.time()
        allowed_difference = self.purgeDays * 24 * 3600 #purge days in seconds
        for s in subdirs: #walk through log subdirectories            
            new_dir = logpath + s
            try:
                stat_result = os.stat(new_dir)
            except OSError:
                cloudLog.warn('Could not stat %s', new_dir, exc_info = True)
                continue
            if stat.S_ISDIR(stat_result.st_mode) and (now - stat_result.st_ctime) > allowed_difference:
                cloudLog.debug('Deleting %s (%s days old)', new_dir, (now - stat_result.st_ctime)/(24*3600))
                try:
                    shutil.rmtree(new_dir)
                except OSError:
                    cloudLog.warn('Could not delete %s', new_dir, exc_info = True)
                     
    
    def update_counter(self, baselogname):
        baselogname = baselogname.replace('<','').replace('>','')
        with self.cntLock:
            cnt = self.pickleCount.get(baselogname,0)
            cnt+=1
            self.pickleCount[baselogname] = cnt
        return cnt
        
    def get_report_file(self, logname, ext, cnt = None, pid = None):
        """Returns the name of a report file with cnt and pid filled in"""
        logname = logname.replace('<','').replace('>','')
       
        mid = ''
        if pid:
            mid += 'P%d.' % pid
        if cnt:
            mid += '%d.' % cnt

        logname = logname % mid
        
        logname+= ext
        
        return self.logPath + logname
        
    
    def open_report_file(self, logname, ext, cnt = None, pid = None):
        """Open an arbitrary report file with cnt and pid filled in"""
        return file(self.get_report_file(logname, ext, cnt, pid),'w')  
        
    
    """Reporting"""
    def save_report(self, dbgserializer, logname, cnt = None, pid = ''):
        
        if not hasattr(dbgserializer,'write_debug_report'):
            #due to serialization level being cloud.call argument, we might not have
            # a write_debug_report in active serializer, even though this object exists
            return
        
        #HACK for default detection
        if type(pid) == str:
            pid = self.pid
         
        reportf = self.open_report_file(logname, '.xml', cnt, pid) 

        dbgserializer.write_debug_report(reportf)
        reportf.close()
        
        return reportf.name

    

        