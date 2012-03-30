"""
Top-level object for PiCloud
Manages caching and client-side type checking
"""
from __future__ import with_statement
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
import threading
import time
import pickle
import collections
import weakref
import types

"""Debug"""
DEBUG = False
if DEBUG:
    import signal
    import traceback
    import os

    def dumpstacks():
        id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
        code = []
        for threadId, stack in sys._current_frames().items():
            code.append("\n# Thread: %s(%d)" % (id2name[threadId], threadId))
            for filename, lineno, name, line in traceback.extract_stack(stack):
                code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
                if line:
                    code.append("  %s" % (line.strip()))
        return '\n'.join(code)
    
    def savestacks(sig, frame):
        print 'debugging..'
        contents = dumpstacks()
        f = open('/tmp/cloud_%s' % os.getpid(), 'wb')
        f.write(contents)
        f.close()    
    
    signal.signal(signal.SIGUSR1, savestacks)
    print 'registered debug for %s' % os.getpid()


try:
    import json    
except ImportError: #If python version < 2.6, we need to use simplejson
    import simplejson as json

try:
    from itertools import izip_longest
except ImportError: #python 2.5 lacks izip_longest
    from .util import izip_longest 
from itertools import izip, chain

from . import cloudconfig as cc
from .cloudlog import cloudLog
from . import serialization

from .util import funcname, validate_func_arguments, fix_time_element 
from .util.cache import JobCacheManager, JobFiniteDoubleCacheManager, \
    JobFiniteSizeCacheManager, JobAbstractCacheManager
from .util.xrange_helper import filter_xrange_list, maybe_xrange_Iter


# set by the instantiation of a Cloud object
_cloud = None

class CloudException(Exception):
    """
    Represents an exception that has occurred by a job on the cloud.
    CloudException will display the job id for failed cloud requests.
    """
    
    status = None
    jid = None
    parameter = None
    
    job_str = 'Job '
    status_str = 'Error '
    
    def __init__(self, value, jid=None, status=None, retry=False, logger=cloudLog):
        self.jid = jid
        self.parameter = value
        self.status = status
        self.retry = retry
        if logger:
            self._logmsg(logger)
        
    def __str__(self):
        if self.jid is not None:
            return self.job_str + str(self.jid) + ': ' + str(self.parameter)
        elif self.status is not None:
            return self.status_str + str(self.status) + ': ' + str(self.parameter)
        else:
            return str(self.parameter)      

    def _logmsg(self, logger):
        """logmsg - only log on jid related"""
        if self.jid is not None:
            logger.warning(''.join(['Job ',str(self.jid),' threw exception:\n ', str(self.parameter)]))
            
class CloudTimeoutError(CloudException):
    """CloudException specifically for join timeouts"""
    
    def __init__(self, value = 'Timed out', jid=None, logger = cloudLog):
        CloudException.__init__(self, value, jid, None, logger)
    
    
    def _logmsg(self, logger):
        """logmsg - only log on jid related"""
        if self.jid is not None:
            logger.warning(''.join(['Job ',str(self.jid),' %s\n ' % self.parameter ]))

def _getExceptionMsg(status):
    """Return exception method associated with a status"""
    if status == 'killed':
        return 'Killed'
    elif status == 'stalled':
        return 'Could not start due to dependency erroring'
    else:
        return 'unknown error'      

class FakeException(Exception):
    pass

#_catchCloudException = CloudException
_catchCloudException = FakeException
_docmsgpkl = "\nPlease see PiCloud documentation for more information about pickle errors"

class Cloud(object):
    """
    Top-level object that manages client-side type checking and client-side cache for Cloud  
    """
       
    job_cache_size = 8191 #cache size for cache manager. None = no cache; 0 = no limit
    result_cache_size = 4096 #cache size for results (applies only if limited jobCache) in bytes; None = no cache; 0 = no limit
    
    finished_statuses = ['done', 'stalled', 'error', 'killed']
    
    # process locally or over network via http
    adapter = None
    
    # manages callbacks
    manager = None
    
    #manages job caching
    cacheManager = None

    __isopen = False
    
    openLock = None
    
    parentModule = None #string representation of module this object is placed in (if any)
    
    __running_on_cloud = cc.transport_configurable('running_on_cloud', default=False, 
                                        comment="Internal. States if running on PiCloud",
                                        hidden = True) 
    
    """compute type constants"""
    c1 = 'c1'
    c2 = 'c2'
    m1 = 'm1'
    s1 = 's1'
        
    @property
    def opened(self): 
        """Returns whether the cloud is open"""
        return self.__isopen
    
    @classmethod
    def running_on_cloud(cls):  #high level check
        """
        Returns true iff current thread of execution is running on PiCloud servers.
        """
        
        return cls.__running_on_cloud
           
    def __init__(self, adapter):
        self.adapter = adapter
        adapter._cloud = self
        self.openLock = threading.RLock()
        self._opening = False
    
    def open (self):
        """Enable the cloud"""
        
        with self.openLock:
            
            if self.opened: #accept this for threading issues
                return                
            
            #hackish re-entry check -- due to issues with local adapter opening this           
            if self._opening:
                return

            try:
                self._opening = True
                if not self.adapter.opened:
                    self.adapter.open()                    
            except _catchCloudException, e:
                raise e
            finally:
                self._opening = False
    
            self.manager = CloudTicketManager(self)
            self.manager.start() 
            
            if self.job_cache_size == None: #no cache
                self.cacheManager = JobAbstractCacheManager()
            elif self.job_cache_size == 0: #no limit on cache
                self.cacheManager = JobCacheManager()            
            else:
                if self.result_cache_size == None:  #no cache
                    resultCacheSize = 0
                elif self.result_cache_size == 0:  #effectively no limit on cache
                    resultCacheSize = sys.maxint
                else:                           #default
                    resultCacheSize = self.result_cache_size
                
                childManager = JobFiniteSizeCacheManager(resultCacheSize,'result')             
                self.cacheManager = JobFiniteDoubleCacheManager(self.job_cache_size, childManager)
                 
            self.__isopen = True
            if not self.adapter.isSlave:
                cloudLog.info('Cloud started with adapter =%s', str(self.adapter))  
    
    
    def close(self):
        """
        Terminate cloud connection (or simulation) and all threads utilized by cloud.
        Returns True if closed cloud; False if cloud was already closed 
        
        .. note::
        
            cloud will automatically re-open if you invoke any cloud.* again
            
        .. warning::
        
            This function should never be called by a job running on PiCloud 
        """    
        if not self.opened:
            return False                   
        if self.adapter.opened:
            self.adapter.close()    
        self.manager.stop()
        cloudLog.info('Cloud closed with adapter =%s', str(self.adapter))
        self.__isopen = False
        return True                
    
    def _checkOpen(self):
        """Open cloud if it is not already"""
        if not self.opened:
            self.open()
    
    def is_simulated(self):
        """
        Returns true iff cloud processor is being run on the local machine.
        """        
        return self.adapter.is_simulated() 
    
    def connection_info(self):
        """
        Returns a dictionary of information describing the current connection.
        """
        return self.adapter.connection_info()

    
    def needs_restart(self, **kwargs):        
        """
        For internal use
        Return true if cloud requires restart based on changed params
        """
        
        if not self.__isopen: #by definition must restart
            return True
        
        return self.adapter.needs_restart(**kwargs)
    
    def _getJobParameters(self, func, mappings, ignore=[]):
        """Return parameters from function and mappings
        Removes keys from mappings handled by this function
        Ignores keys in ignore[]"""
        
        # defaults
        job_label = None
        job_priority = 5
        job_restartable = True
        _type = 'c1'
        cores = 1
        profile = False
        depends_on = []
        depends_on_errors = 'abort'
        fast_serialization = 0
        kill_process = False #not documented yet
        env = None
        vol = []
        max_runtime = None
        
        if '_label' in mappings and '_label' not in ignore:
            #job_label must be an ASCII string
            try:
                job_label = mappings['_label'].decode('ascii').encode('ascii')
            except (UnicodeDecodeError, UnicodeEncodeError):
                raise TypeError('_job_label must be an ASCII string')
            del mappings['_label']
        
        if '_priority' in mappings and '_priority' not in ignore:
            job_priority = mappings['_priority']
            if not isinstance(job_priority, (int, long)):
                raise TypeError(' _priority must be an integer')             
            del mappings['_priority']

        if '_restartable' in mappings and '_restartable' not in ignore:
            job_restartable = mappings['_restartable']
            if not isinstance(job_restartable, bool):
                raise TypeError(' _job_restartable must be boolean')             
            del mappings['_restartable']
        
        if '_depends_on' in mappings and '_depends_on' not in ignore:
            if hasattr(mappings['_depends_on'],'__iter__'):
                depends_on = mappings['_depends_on']
            elif isinstance (mappings['_depends_on'], (int,long)):
                depends_on = [mappings['_depends_on']]
            else:
                raise TypeError('_depends_on must be a jid or list of jids')
            del mappings['_depends_on']
            
            # Validate depends_on
            if not isinstance(depends_on, xrange):
                test = reduce(lambda x, y: x and (isinstance(y,(int,long,xrange))), depends_on, True)
                if not test:
                    raise TypeError( '_depends_on list can only contain jids' )

        if '_depends_on_errors' in mappings and '_depends_on_errors' not in ignore:
            if isinstance(mappings['_depends_on_errors'], basestring):
                depends_on_errors = str(mappings['_depends_on_errors'])
            else:
                raise TypeError('_depends_on_errors must be a string')
            if depends_on_errors not in ['ignore', 'abort']:
                raise ValueError('_depends_on_errors must be "abort" or "ignore"')
            del mappings['_depends_on_errors']
        
        if '_fast_serialization' in mappings  and '_fast_serialization' not in ignore:
            fast_serialization = mappings['_fast_serialization']
            
            if not isinstance(fast_serialization, (int, long)):
                raise TypeError(' _fast_serialization must be an integer')
            elif isinstance(fast_serialization, types.BooleanType):
                raise TypeError(' _fast_serialization must be an integer')
                                    
            del mappings['_fast_serialization']    
        
        if '_type' in mappings and '_type' not in ignore:
            if isinstance(mappings['_type'], basestring):
                _type = str(mappings['_type'])            
            else:
                raise TypeError('_type must be a string')
            del mappings['_type']
        
        if '_high_cpu' in mappings  and '_high_cpu' not in ignore:
            #TODO: Warn that this is deprecated
            if isinstance(mappings['_high_cpu'], bool):
                high_cpu = mappings['_high_cpu']
                if high_cpu:
                    _type = 'c2'
            else:
                raise TypeError('_high_cpu must be a boolean')
            del mappings['_high_cpu']
        
        if '_cores' in mappings  and '_cores' not in ignore:
            if isinstance(mappings['_cores'], (int, long)):
                cores = mappings['_cores']
            else:
                raise TypeError('_cores must be a natural number')
            del mappings['_cores']
            
        if '_env' in mappings and  '_env' not in ignore:
            env = str(mappings['_env'])
            del mappings['_env']
            
        if '_vol' in mappings and  '_vol' not in ignore:
            vol = mappings['_vol']
            if not hasattr(vol, '__iter__'):
                vol = [vol]
            for e in vol:
                if not isinstance(e, basestring):
                    raise TypeError('_vol must be a string or a list of strings')
            del mappings['_vol']

        if '_max_runtime' in mappings and  '_max_runtime' not in ignore:
            if isinstance( mappings['_max_runtime'], (int, long) ):
                max_runtime = mappings['_max_runtime']
                if max_runtime <= 0:
                    raise ValueError('_max_runtime must be an integer > 0')
            else:
                raise TypeError('_max_runtime must be an integer')
            
            del mappings['_max_runtime']

        if '_profile' in mappings and  '_profile' not in ignore:
            profile = bool( mappings['_profile'])
            del mappings['_profile']
            
        if '_kill_process' in mappings and '_kill_process' not in ignore:
            kill_process = bool( mappings['_kill_process'])
            del mappings['_kill_process']

        
        parameters = {'fast_serialization': fast_serialization,
                      'func_name': funcname(func),
                      'label': job_label,
                      'priority': job_priority,
                      'restartable': job_restartable,
                      'type': _type,
                      'cores': cores,
                      'env' : env,
                      'vol' : vol,
                      'max_runtime': max_runtime,
                      'profile': profile,
                      'depends_on': depends_on,
                      'depends_on_errors' : depends_on_errors,
                      'kill_process' : kill_process
                      }
        
        for ig_key in ignore:
            if ig_key in parameters:
                del parameters[ig_key]
        
        return parameters
    
    def _check_jid_type(self, jids):
        if not isinstance(jids, xrange):
            test = reduce(lambda x, y: x and (isinstance(y,(int,long,xrange))), jids, True)
            if not test:
                raise TypeError( 'jid(s) must be integers' )
    
    def call(self, func, *args, **kwargs):
        """       
        Invoke *func* (a callable) in the cloud.
        When invoked, *func* will be invoked on PiCloud's cluster with the passed 
        *args* that follow it.  The invoked function is known as a 'job'.  The
        return value of the invoked function is known as the 'internal result'.

        Call will return an integer Job IDentifier (jid) which can be passed into 
        the status and result methods to obtain the status of the job and the 
        internal result respectively.
        
        Example::    
        
            def mult3(x):
                return 3*x
            cloud.call(mult3, 2) 
        
        This will cause mult3 to be invoked on PiCloud's cluster with x=2
        
        See online documentation for additional information.
        
        Reserved special *kwargs* (see docs for details):
            
        * _callback: 
            A list of functions that should be run on the callee's computer once
            this job finishes successfully.
        * _callback_on_error:  
            A list of functions that should be run on the callee's computer if this
            job errors.  
        * _depends_on:
            An iterable of jids that represents all jobs that must complete successfully 
            before the job created by this call function may be run.
        * _depends_on_errors:
            A string specifying how an error with a jid listed in _depends_on should be handled.
            'abort': Set this job to 'stalled' (Default)
            'ignore': Treat an error as satisfying the dependency
        * _env:
            A string specifying a custom environment you wish to run your job within.
            See environments overview at 
            http://blog.picloud.com/2011/09/26/introducing-environments-run-anything-on-picloud/                
        * _fast_serialization:
            This keyword can be used to speed up serialization, at the cost of some functionality.
            This affects the serialization of both the arguments and return values
            *func* will always be serialized by the enhanced serializer, with debugging features.
            Possible values keyword are:
                        
            0. default -- use cloud module's enhanced serialization and debugging info            
            1. no debug -- Disable all debugging features for arguments            
            2. use cPickle -- Use Python's fast serializer, possibly causing PicklingErrors                
                    
        * _kill_process:
            Terminate the Python interpreter *func* runs in after *func* completes, preventing
            the interpreter from being used by subsequent jobs.  See Technical Overview for more info.                
        * _label: 
            A user-defined string label that is attached to the job. Labels can be
            used to filter when viewing jobs interactively (i.e. on the PiCloud website).
        * _max_runtime:
            Specify the maximum amount of time (in integer minutes) a job can run. If job runs beyond 
            this time, it will be killed.                      
        * _priority: 
            A positive integer denoting the job's priority. PiCloud tries to run jobs 
            with lower priority numbers before jobs with higher priority numbers.                
    	* _profile:
    	    Set this to True to enable profiling of your code. Profiling information is 
    	    valuable for debugging, but may slow down your job.
        * _restartable:
            In the very rare event of hardware failure, this flag indicates that the job
            can be restarted if the failure happened in the middle of the job.
            By default, this is true. This should be unset if the job has external state
            (e.g. it modifies a database entry)                   
        * _type:
            Select the type of compute resources to use.  PiCloud supports four types,
            specified as strings:
            
            'c1'
                1 compute unit, 300 MB ram, low I/O (default)                    
            'c2'
                2.5 compute units, 800 MB ram, medium I/O                    
            'm1'                    
                3.25 compute units, 8 GB ram, high I/O
            's1'
                Up to 2 compute units (variable), 300 MB ram, low I/O, 1 IP per core
                           
            See http://www.picloud.com/pricing/ for pricing information
        * _vol:
            A string specifying a volume you wish your job to have access to. 
        """            
        
        self._checkOpen()

        if not callable(func):
            raise TypeError( 'cloud.call first argument (%s) is not callable'  % (str(func) ))
        
        parameters = self._getJobParameters(func, kwargs)
        
        if '_callback' in kwargs:
            callback = kwargs['_callback']
            del kwargs['_callback']
        else:
            callback = None
            
        if '_callback_on_error' in kwargs:
            callback_on_error = kwargs['_callback_on_error']        
            del kwargs['_callback_on_error']
        else:
            callback_on_error = None       
            
        validate_func_arguments(func, args, kwargs) 
        
        try:
            jid = self.adapter.job_call(parameters,func,args,kwargs)
        except _catchCloudException, e:
            raise e    
        except pickle.PicklingError, e:            
            e.args = (e.args[0] + _docmsgpkl,)            
            raise e
              
        # only add ticket to manager (will poll to see whether job has
        # finished) if there is a callback assigned to the call
        if callback:            
            self.manager.add_callbacks(jid, callback, 'success')
        if callback_on_error:
            self.manager.add_callbacks(jid, callback_on_error, 'error')        

        return jid    
    
    def join(self, jids, timeout = None, ignore_errors = False):
        """
        Block current thread of execution until the job specified by the integer *jids*
        completes.  Completion is defined as the job finishing or erroring
        (including stalling).          
        If the job errored, a CloudException detailing the exception that triggered
        the error is thrown. If multiple errors occur, it is undefined which job's exception
        will be raised.
        If the job does not exist, a CloudException is thrown.
        
        This method also accepts an iterable describing *jids* and blocks until all
        corresponding jobs finish. If an error is seen, join may terminate before 
        all jobs finish.
        
        If *timeout* is set to a number, join will raise a CloudTimeoutError if the
        job is still running after *timeout* seconds
        
        If *ignore_errors* is True, no CloudException will be thrown if a job errored.
        Join will block until every job is complete, regardless of error status.
        """
        
        #TODO: Use cloud ticket manager with this
        poll_interval = 1.0
        
        self._checkOpen()
                
        if not hasattr(jids,'__iter__'):
            jids = [jids] 
        
        self._check_jid_type(jids)        
        
        #variables filter can modify:
        #NOTE: With Python3, they can just be nonlocal
        #Some of this code is legacy
        class FilterSideEffects:
            isDone = True           #if jobs are all done up to the job we are reading
            seenException = False   #if an exception has been seen
            aptr = 0                #Pointer into returned statuses/exceptions            
        
        def filterJob(jid, status):
            """Helper function to filter out done jobs and fire exceptions if needed                 
            """                        
            if status != 'done': 
                if status in self.finished_statuses:
                    if ignore_errors:
                        return False
                    print '===='
                    print 'jid:', jid
                    exception = self.info(jid,'exception')[jid]['exception']
                    print 'exception:', exception
                    print '====' 
                    if status == 'error' or (status == 'killed' and exception):
                        raise CloudException(exception, jid=jid, status=status)
                    else:
                        msg = _getExceptionMsg(status)
                        raise CloudException(msg, jid=jid, status=status)  
                return True  #keep iterating
            return False            
       
        cachedStatuses = self.cacheManager.getCached(jids, ('status',))
       
        def cacheJobFilter(jid):
            """Filter out cached jobs"""
            status = cachedStatuses.get(jid,{}).get('status',None)
            if status:
                return filterJob(jid, status)
            else: #job not in cache
                return True  
        
        neededJids = filter_xrange_list(cacheJobFilter,jids)

        time_ctr = 0
        while True:
            
            if neededJids:
                try:
                    astatuses = self.adapter.jobs_join(neededJids, timeout)
                    if astatuses is False: #adapter does not support jobs_join:
                        astatuses = self.status(neededJids)
                except _catchCloudException, e:
                    raise e                             
            else:
                break 
            
            fse = FilterSideEffects() #used by below function                        
            def resultFilter(jid):
                """Filter out jobs we got the status for"""            
                status = astatuses[fse.aptr]
                fse.aptr += 1
                
                return filterJob(jid, status) #this can raise exceptions
            
            neededJids = filter_xrange_list(resultFilter, neededJids)            
            
            if not neededJids:
                break

            if timeout and time_ctr > timeout:
                raise CloudTimeoutError('cloud.join timed out', neededJids)
            
            time.sleep(poll_interval) #poll
            time_ctr+= poll_interval
            
    def status(self, jids):
        """
        Returns the status of the job specified by the integer *jids*. If 
        the job does not exist, a CloudException is thrown.
        This method also accepts an iterable describing *jids*, in which case a respective list
        of statuses is returned."""
        #really a wrapper for cloud.info
        deseq = False
        if not hasattr(jids,'__iter__'):
            jids = [jids]
            deseq = True
                
        status_dict = self.info(jids,'status')
        
        if deseq:
            return status_dict[jids[0]]['status']
        else:
            return [status_dict[jid]['status'] for jid in maybe_xrange_Iter(jids)]
        
    def result(self, jids, timeout = None, ignore_errors = False):
        """
        Blocks until the job specified by the integer *jids* has completed and
        then returns the internal result of the job.         
        If the job errored, a CloudException detailing the exception that triggered
        the error is thrown. 
        If the job does not exist, a CloudException is thrown.        

        This function also accepts an iterable describing *jids*, in which case a respective list
        of internal results is returned
        
        If *timeout* is set to a number, result will raise a CloudTimeoutError if the
        job is still running after *timeout* seconds
        
        If *ignore_errors* is True, a job that errored will not raise an exception.
        Instead, its return value is the CloudException describing the error.
        """        
        return self.__result(jids, timeout, ignore_errors)
 
    def iresult(self, jids, timeout = None, num_in_parallel=10, ignore_errors = False):
        """
        Similar to result, but returns an iterator that iterates, in order, through  
        the internal results of *jids* as the respective job finishes, allowing quicker 
        access to results and reducing memory requirements of result reading.
        
        If a job being iterated over errors, an exception is raised. 
        However, the iterator does not exhaust; if the exception is caught, one can continue
        iterating over the remaining jobs.
        
        If *timeout* is set to a number, a call to the iterator's next function 
        will raise a CloudTimeoutError after *timeout* seconds if no result becomes available.
        
        *num_in_parallel* controls how many results are read-ahead from the cloud
        Set this to 0 to use the allowed maximum.
        
        If *ignore_errors* is True, a job that errored will return the CloudException describing 
        its error, rather than raising an Exception        
        """        
        return self.__iresult(jids, timeout, num_in_parallel, ignore_errors)
         
    def __result(self, jids, timeout = None, ignore_errors = False, by_jid=False):
        deseq = False
        if not hasattr(jids,'__iter__'):
            jids = [jids]
            deseq = True

        self._check_jid_type(jids)

        self._checkOpen()

        if not by_jid:
            try:
                self.join(jids, timeout = timeout, ignore_errors=ignore_errors)  # wait for jids to be ready
            except _catchCloudException, e:
                raise e            

        #everything is now cached AND done

        if by_jid:
            cached_results = {}
        else:
            cached_results  = self.cacheManager.getCached(jids, ('result',))        
        needed_jids = filter_xrange_list(lambda jid: jid not in cached_results,jids)
        
        jid_exceptions = {}
        
        if ignore_errors:
            #as ignore_errors is a rarely used flag, this block is not fully optimized                        
            exceptions_dct = self.info(jids, ['exception', 'status'])
            
            for jid, excp_dct in exceptions_dct.iteritems():
                status = excp_dct['status']
                if status != 'done':
                    exception = excp_dct['exception']
                    if not exception:
                        exception = _getExceptionMsg(status)
                    jid_exceptions[jid] = (status, exception)
            needed_jids = filter_xrange_list(lambda jid: jid not in jid_exceptions, jids)                 

        if needed_jids:
            try:
                resp = self.adapter.jobs_result(jids=needed_jids, by_jid=by_jid)
                
            except _catchCloudException, e:
                raise e                                           
            
            results_list = resp['data']
            interpretation = resp['interpretation']

            aresults = izip_longest(interpretation, results_list)

        else:
            aresults = []

        a_result_iter = aresults.__iter__()
        
        outresults = []
        
        for jid in maybe_xrange_Iter(jids):
            
            if cached_results.has_key(jid):            
                result = cached_results.get(jid)['result']
                outresults.append(serialization.deserialize(result))
            
            elif jid_exceptions.has_key(jid):
                status, exception = jid_exceptions.get(jid)
                clexp = CloudException(exception, jid=jid, status=status, logger=None)                
                outresults.append(clexp)
                continue
            
            else:
                interpret, result = a_result_iter.next()

                if interpret['datatype'] == 'python_pickle':
                    outresults.append(serialization.deserialize(result))
                    if not by_jid:
                        self.cacheManager.putCached(jid, status='done', result=result)
                                                
                elif interpret['datatype'] == 'json':
                    if interpret['action'] == 'cloud.iresult_global':
                        sub_jids = json.loads(result)
                        sub_results_iter = chain.from_iterable( self.__iresult(sub_jids, num_in_parallel=1, by_jid=True) )
                        outresults.append(sub_results_iter)
                        
        if deseq:
            return outresults[0]
        else:
            return outresults 
   
    def __iresult(self, jids, timeout = None, num_in_parallel=10, ignore_errors = False, by_jid=False):
        if not hasattr(jids,'__iter__'):
            jids = [jids]
        
        num_jids = len(jids)
        
        max_in_parallel = 1024
        if num_in_parallel < 1 or num_in_parallel > max_in_parallel:
            num_in_parallel = max_in_parallel
        
        self._checkOpen()
                            
        ready_results = collections.deque()  #queue of results
        result_cv = threading.Condition()  #both main and loop threads sleep on this
        errorJid = []  #1st element set to a job if an error is to be returned
        isDone = [] #1st element set to True when result_loop finishes. Set to an exception if result_loop crashed
        
        def result_loop(iterator_ref):
            """
            Helper function run inside another thread
            It downloads results as the main thread continues work
            
            Slight issue is present in that if the condition variable is not necessarily
            waited on if an error is present in the ready_results buffer
            """
            jidIterator = maybe_xrange_Iter(jids)
            jids_testing = collections.deque()   #queue of jobs being tested            
            poll_interval = 1.0
            
            try:
                while self.__isopen:                
                    #First add jobs to check
                    try:
                        while len(jids_testing) < num_in_parallel:
                            jids_testing.append(jidIterator.next())
                    except StopIteration:
                        if not jids_testing:  #loop is done!
                            cloudLog.debug('cloud.iresult.result_loop has completed')
                            isDone.append(True)
                            break 
                    
                    with result_cv:
                        while True:
                            if not iterator_ref(): #GC Detection on iterator removal
                                cloudLog.debug('cloud.iresult.result_loop detected garbage collection')
                                break
                            
                            if (len(ready_results) < num_in_parallel and not errorJid):
                                break
                            result_cv.wait(5.0)  #poll for Garbage collection
                      
                    if not iterator_ref():  #GC Detection on iterator removal
                        break
                    assert (len(ready_results) < num_in_parallel)
                    assert (not errorJid)
                         
                    if by_jid:
                        statuses = ['done' for _ in xrange(num_jids)]
                    else:
                        statuses = self.status( jids_testing )
                        cloudLog.debug('cloud.__iresult.result_loop testing %s. got status %s' % \
                                       (jids_testing, statuses))
                    ctr = 0
                    jids_to_access = []
                
                    #find all done jobs whose results we can grab
                    for jid in jids_testing:                    
                        status = statuses[ctr]
                        if status not in self.finished_statuses:
                            break
                        if status == 'done':                    
                            jids_to_access.append(jid)
                        else: 
                            #postpone error handling until all good results handled:
                            if not jids_to_access and not ready_results:                                                                                 
                                errorJid.append(jid)
                            break
                        ctr+=1
                    #technically race condition here -- what if error_jid is popped?
                    if errorJid: #on error, return to top. Top will wait on cv
                        jids_testing.popleft()
                        with result_cv:
                            result_cv.notify()
                        continue 
                    
                    if not jids_to_access: #no jobs ready yet -- wait and return to top
                        time.sleep(poll_interval)  
                        #FIXME: Should use adapter.join
                        continue                
                
                    if not by_jid:
                        cloudLog.debug('cloud.__iresult.result_loop getting results of %s' % jids_to_access)

                    new_results  = self.__result(jids_to_access, by_jid=by_jid) 
                    with result_cv:
                        for result in new_results:
                            ready_results.append(result)
                            jids_testing.popleft()
                        result_cv.notify()

                cloudLog.debug('cloud.__iresult.result_loop is terminating')
                with result_cv:
                    result_cv.notify()
            
            except Exception, e:
                cloudLog.exception('cloud.iresult.result_loop crashed')
                isDone.append(e)
                with result_cv:
                    result_cv.notify()
 

        class ResultIterator(object):
            """Iterator that handles results"""
            
            def __init__(self, parentInstance):
                self.parent = parentInstance              
                self.cnt = 0
                self.crashed = False
            
            def __iter__(self):
                return self
            
            def _checkDone(self):
                if isDone:
                    cloudLog.debug('cloud.__iresult.ResultIterator is done.')
                    if isinstance(isDone[0], Exception):  #something went wrong
                        cloudLog.error('ResultIterator detected that polling thread crashed')
                        self.crashed = True #if next is ever called again, raise StopIteration
                        raise isDone[0]
                    raise StopIteration
                
            def next(self, timeout = timeout):
                """Just walk through ready_results"""     
                
                if self.crashed:
                    raise StopIteration
          
                with result_cv:                    
                    if not errorJid and not ready_results:
                        self._checkDone()
                        cloudLog.debug('cloud.__iresult.ResultIterator is going to sleep')
                        result_cv.wait(timeout)
                        if not errorJid and not ready_results:
                            self._checkDone()
                            
                            #we timed out
                            raise CloudTimeoutError('__iresult iterator timed out')
                        cloudLog.debug('cloud.__iresult.ResultIterator is awake. num ready results = %s errorJid is %s' % (len(ready_results), errorJid))
                    
                    result_cv.notify() #waken result_loop thread
                    
                    if errorJid:  #trigger exception with join:
                        try:
                            self.parent.join(errorJid)
                        except CloudException, e:
                            errorJid.pop()  #result thread can now continue
                            if ignore_errors:
                                return e
                            else:
                                raise e
                        else: #should never occur
                            assert(False) 
                        
                    assert(ready_results)
                    return ready_results.popleft()
                    
        ri = ResultIterator(self)
        result_thread = threading.Thread(target=result_loop, args=(weakref.ref(ri),))  
        result_thread.daemon = True
        result_thread.name = 'cloud.iresult.result_loop'
        result_thread.start()  
        
        return ri
    
    def iresult_unordered(self, jids):
        """Hackish - probably should not use"""
        
        class UnorderedResultIterator(object):
            """Iterator that handles results"""
            
            def __init__(self, parent):              
                self.jids_testing = list(maybe_xrange_Iter(jids))
                self.parent = parent
                
            
            def __iter__(self):
                return self
            
            def next(self):
                                
                while True:
                    
                    jids = self.jids_testing                
                    statuses = self.parent.status( jids )                
                                
                    for jid, status in zip(jids, statuses):
                        if status in self.parent.finished_statuses:
                            self.jids_testing.remove(jid)
                            
                            if status == 'done': #ignore errors
                                res = self.parent.__result(jid)                            
                                return jid, res
                    
                    if self.jids_testing:
                        time.sleep(0.5)
                    else:
                        raise StopIteration
        
        return UnorderedResultIterator(self)
               
    def info(self, jids, info_requested=None):
        """        
        Request information about jobs specified by integer or iterable *jids*
        
        As this function is designed for console debugging, the return value 
        is a dictionary whose keys are the *jids* requested.  The values are themselves
        dictionaries, whose keys in turn consist of valid values within the iterable
        *info_requested*.  Each key maps to key-specific information
        about the job, e.g. stdout maps to standard output of job.

        Possible *info_requested* items are one of more of:
        
        * status: Job's status
        * stdout: Standard out produced by job (last 64k characters)
        * stderr: Standard err produced by job (last 64k characters)
        * exception: Any exception raised
        * runtime: runtime (wall clock) of job, specifically how long function took to evaluate
        * created: datetime when job was created on picloud by a call/map
        * finished: datetime when job completed running. note that finished - created = runtime + time in queue
        * code_version: unique id mapping to version of code this job runs under
        * env: Environment job runs under
        * vol: list of volumes job runs under
        * profile: If _profile=True when the job was created, the profile of the function that ran
        
        If *info_requested* is None, info_requested will be status, stdout, stderr, and runtime.
        
        profile and finished only have data once job has finished. All others can be accessed while running                
	    
	    e.g. cloud.info(42,'stdout') to get standard output of job 42
        """
                
        if not hasattr(jids,'__iter__'):
            jids = [jids]
        
        self._check_jid_type(jids)                
        
        if not info_requested:
            info_requested = ['stdout', 'stderr', 'status', 'runtime']
        
        if not hasattr(info_requested,'__iter__'):
            info_requested = [info_requested]
        
        test = reduce(lambda x, y: x and (isinstance(y,str)), info_requested, True)   
        if not test:
            raise TypeError( 'info_requested must be a valid string value or list of strings' )         

        self._checkOpen()   
        
        cached_info  = self.cacheManager.getCached(jids, info_requested)
        
        #As we only do a single request, we must request all information for any job missing any requested info items
        info_len = len(info_requested)
        needed_jids = filter_xrange_list(lambda jid: len(cached_info.get(jid,[])) != info_len,jids)     
        
        if needed_jids:
            try:
                ainfo_map = self.adapter.jobs_info(jids=needed_jids,info_requested=info_requested)           
            except _catchCloudException, e:
                raise e 
            
            if 'status' not in info_requested:
                cached_statuses = self.cacheManager.getCached(jids, ('status',))
            else:
                cached_statuses = None
        
            can_cache = True
            for jid in ainfo_map.iterkeys():
                #TODO: Correctly cache file_map meta jobs. For now, we must skip caching
                if isinstance(jid, basestring) and 'filemap' in jid:
                    can_cache = False
                    break
                 
            if can_cache:
                for jid, ainfo in ainfo_map.iteritems():
                    status = ainfo.get('status') or cached_statuses.get(jid,{}).get('status',None)
                    if status in self.finished_statuses:
                        self.cacheManager.putCached(jid, **(ainfo))
                       
            cached_info.update(ainfo_map)
        
        for info in cached_info.values():
            if 'created' in info_requested:
                fix_time_element(info, 'created')
            if 'finished' in info_requested:
                fix_time_element(info, 'finished')

        return cached_info 
    
    def kill(self, jids=None):
        """
        Kill any incomplete jobs specified by the integer or iterable *jids*.
        If no arguments are specified (or jids is None), kill every job that has been submitted.
        """
        
        #Internal Note: No caching takes place here, due to rarity of this command
                   
        if jids!=None:
            if not hasattr(jids,'__iter__'):
                jids = [jids]
            
            self._check_jid_type(jids)
            
            if hasattr(jids,'__len__') and len(jids) == 0:
                return

        self._checkOpen()
                
        try:
            self.adapter.jobs_kill(jids)
        except _catchCloudException, e:
            raise e    
    
    def delete(self, jids):
        """
        Remove all data (result, stdout, etc.) related to jobs specified by the 
        integer or iterable *jids* 
        Jobs must have finished already to be deleted.
        
        .. note::
            
            In MP/Simulator mode, this does not delete any datalogs, only in-memory info
        """
        if not hasattr(jids,'__iter__'):
            jids = [jids]
        
        self._check_jid_type(jids)        
        self._checkOpen()
                    
        try:
            self.adapter.jobs_delete(jids)
        except _catchCloudException, e:
            raise e
        finally:
            self.cacheManager.deleteCached(jids)
    
    def map(self, func, *args, **kwargs):
        """
        Map *func* (a callable) over argument sequence(s).
        cloud.map is meant to mimic a regular map call such as::
            
            map(lambda x,y: x*y, xlist, ylist)
        
        *args* can be any number of iterables. If the iterables are of different
        size, 'None' is substituted for a missing entries.    
        
        Map will return an iterable describing integer Job IDentifiers (jids).  Each jid
        corresponds to func being invoked on one item of the sequence(s) (a 'job').
        In practice, the jids can (and should) be treated as a single jid;
        the returned iterable may be passed directly into status, result, etc.
        
        Using cloud.result on the returned jids will return a list of internal results 
        (each being the result of applying the function to an item of the argument sequence(s)).
        
        Example::
    
            def mult(x,y):
                return x*y
            jids = cloud.map(mult, [1,3,5], [2,4,6]) 
    
        This will cause mult3 to be invoked on PiCloud's cluster with x=2
        
        Results::
    
            cloud.result(jids)
            >> [2,12,30]
        
        The result is [1*2,3*4,5*6]
        
        See online documentation for additional information    
        
        Reserved special *kwargs* (see docs for details):
        
        * _depends_on:
            An iterable of jids that represents all jobs that must complete successfully 
            before any jobs created by this map function may be run.
        * _depends_on_errors:
            A string specifying how an error with a jid listed in _depends_on should be handled.
            'abort': Set this job to 'stalled'  (Default)
            'ignore': Treat an error as satisfying the dependency
        * _env:
            A string specifying a custom environment you wish to run your jobs within.
            See environments overview at 
            http://blog.picloud.com/2011/09/26/introducing-environments-run-anything-on-picloud/
        * _fast_serialization:
            This keyword can be used to speed up serialization, at the cost of some functionality.
            This affects the serialization of both the map arguments and return values
            The map function will always be serialized by the enhanced serializer, with debugging features.
            Possible values keyword are:
                        
            0. default -- use cloud module's enhanced serialization and debugging info            
            1. no debug -- Disable all debugging features for arguments            
            2. use cPickle -- Use python's fast serializer, possibly causing PicklingErrors                
        * _kill_process:
                Terminate the Python interpreter *func* runs in after *func* completes, preventing
                the interpreter from being used by subsequent jobs.  See Technical Overview for more info.                            
        * _label: 
            A user-defined string label that is attached to the created jobs. 
            Labels can be used to filter when viewing jobs interactively (i.e.
            on the PiCloud website).
        * _max_runtime:
            Specify the maximum amount of time (in integer minutes) a job can run. If job runs beyond 
            this time, it will be killed.                                              
        * _priority: 
                A positive integer denoting the job's priority. PiCloud tries to run jobs 
                with lower priority numbers before jobs with higher priority numbers.            
        * _profile:
                Set this to True to enable profiling of your code. Profiling information is 
                valuable for debugging, but may slow down your job.
        * _restartable:
                In the very rare event of hardware failure, this flag indicates that the job
                can be restarted if the failure happened in the middle of the job.
                By default, this is true. This should be unset if the job has external state
                (e.g. it modifies a database entry)
        * _type:
                Select the type of compute resources to use.  PiCloud supports four types,
                specified as strings:
                
                'c1'
                    1 compute unit, 300 MB ram, low I/O (default)                    
                'c2'
                    2.5 compute units, 800 MB ram, medium I/O                    
                'm1'                    
                    3.25 compute units, 8 GB ram, high I/O
                's1'
                    variable compute units (2 cu max), 300 MB ram, low I/O, 1 IP per core                    
                                    
                See http://www.picloud.com/pricing/ for pricing information        
        """
        
        #TODO: Add split/chunking param.. see multiprocessing
        if not callable(func):
            raise CloudException( 'cloud.map first argument (%s) is not callable'  % (str(func) ))
        
        if len(args) == 0:
            raise CloudException('cloud.map needs at least 2 arguments')
        
        self._checkOpen()
                
        parameters = self._getJobParameters(func, kwargs)
        
        #typecheck and access correct iterator:
        argIters = []
        cnt = 1
        map_len = 0 #length of this map (i.e. jobs).  None if cannot be determined
        for arg in args:
            try:
                argIters.append(maybe_xrange_Iter(arg, coerce = False))
            except AttributeError:
                raise TypeError( 'map argument %d (%s) is not an iterable'  % (cnt,str(arg) ))
            else:
                cnt +=1
                if map_len != None and hasattr(arg, '__len__'):
                    map_len = max(len(arg), map_len)
                else:
                    map_len = None
        
        if map_len > 10000: #hard limit
            raise CloudException('No more than 10,000 arguments can be submitted in cloud.map')
        
        parameters['map_len'] = map_len 
        
        #[(a,b,c), (1,2,3)] --> [(a,1), (b,2), (c,3)]
        argList = izip_longest(*argIters)
       
        try:
            jids = self.adapter.jobs_map(params=parameters,func=func,mapargs=argList)
        except _catchCloudException, e:
            raise e    
        
        except pickle.PicklingError, e:
            e.args = (e.args[0] + _docmsgpkl,)
            raise e
        
        
        # TODO: ADD SUPPORT FOR CALLBACKS!
        
        return jids
    
class CloudTicketManager(threading.Thread):
    """
    CloudTicketManager is responsible for managing callbacks
    associated with specific jobs/tickets.
    """
    
    cloud = None
    
    # list of tickets to 
    pending_tickets = None
    
    # condition variable
    cv = None
    
    # dict mapping ticket -> callbacks on success
    _callbacks_on_success = None
    
    # dict mapping ticket -> callbacks on error
    _callbacks_on_error = None
    
    # event indicating if thread should die
    die_event = None
    
    def __init__(self, cloud):
        threading.Thread.__init__(self)
    
        self.cloud = cloud
        self.pending_tickets = []
        self.cv = threading.Condition()
        
        # make this a daemon thread
        self.setDaemon(True)
        
        self._callbacks_on_success = {}
        self._callbacks_on_error = {}
        self.die_event = threading.Event()
        
    def stop(self): 
        self.die_event.set()
        
        with self.cv:    
            self.cv.notify()
        
        self.join() #block until shut down
    
    def add_callbacks(self, ticket, callbacks, type='success'):
        """
        Callbacks should be a function or a list of functions that
        take as its first argument the return value of the cloud call.
        Callbacks will be called in order from left to right. 
        
        Returns True if callbacks are added, False otherwise. 
        """

        # filters empty lists
        if not callbacks:
            return False
                
        # if callbacks is a function, make it a one item list
        # otherwise, assume it is a list of functions
        if not hasattr(callbacks,'__iter__'):
            callbacks = [callbacks]

        if not reduce(lambda x, y: x and (callable(y)), callbacks, True):
            raise CloudException( 'All callbacks must be callable')
        
        # add to different trigger list depending on type
        if type == 'success':
            self._callbacks_on_success.setdefault(ticket, []).extend(callbacks)
        elif type == 'error':
            self._callbacks_on_error.setdefault(ticket, []).extend(callbacks)
        else:
            raise Exception('Unrecognized callback type.')
    
        # add ticket to watch list
        with self.cv:
            # only add if ticket is not currently present
            if ticket not in self.pending_tickets:
                self.pending_tickets.append(ticket)
                self.cv.notify()
        
        return True
    
    def run(self):
        
        while not self.die_event.isSet():
            with self.cv:
                
                while len(self.pending_tickets) == 0 and not self.die_event.isSet():                                    
                    self.cv.wait()
                
                if self.die_event.isSet():
                    return                
            
            # ASSUMPTION: This is the only thread that will remove tickets
            # TODO: If job does not finish for some time, increase sleep interval
            time.sleep(2.0)
            
            with self.cv:
                
                # get status of tickets
                statuses = self.cloud.status(self.pending_tickets)
                
                # for each ticket, call callbacks if in a finished state
                for ticket, status in zip(self.pending_tickets, statuses):
                    
                    if status in Cloud.finished_statuses:
                        
                        # call appropriate callback
                        callbacks = self._callbacks_on_success.get(ticket,[]) if status == 'done' else self._callbacks_on_error.get(ticket,[])
         
                        # call callback with function argument if it takes an argument
                        for callback in callbacks:
                            if callback.func_code.co_argcount > 0:
                                callback(ticket)
                            else:
                                callback()
                        
                        # callback triggered -> remove ticket from watch list
                        self.pending_tickets.remove(ticket)
                    
