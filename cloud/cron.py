"""
For managing crons on PiCloud.  
Crons allow you to "register" a function to be invoked periodically on PiCloud
according to a schedule you specify.

Api keys must be configured before using any functions in this module.
(via cloudconf.py, cloud.setkey, or being on PiCloud server)
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

import cloud

from .util import min_args
from .util.zip_packer import Packer
from .util.cronexpr import CronTime

_register_query = 'cron/register/'
_deregister_query = 'cron/deregister/'
_run_query = 'cron/run/'
_list_query = 'cron/list/'
_info_query = 'cron/info/'

"""
This module utilizes the cloud object extensively
The functions can be viewed as instance methods of the Cloud (hence accessing of protected variables)
"""


def register(func, label, schedule, **kwargs):
    """       
    Register *func* (a callable) to be run periodically on PiCloud according to *schedule*
    
    The cron can be managed in the future by the specified *label* 
    
    *Schedule* is a BSD-style cron timestamp, specified as either a string of 5 non-
    whitespace characters delimited by spaces or as a sequence (tuple, list, etc.) of 
    5 elements, each with non-whitespace characters.
    The ordering of the characters represent scheduling information for minutes, 
    hours, days, months, and days of week.                   
    See full format specification at http://unixhelp.ed.ac.uk/CGI/man-cgi?crontab+5        
    Year is not supported.  The N/S increment format is though.
                    
    PiCloud schedules your crons under the *GMT (UTC+0) timezone*.

    Certain special *kwargs* associated with cloud.call can be attached to the periodic jobs: 

    * _env:
        A string specifying a custom environment jobs should use
        See environments overview at 
        http://blog.picloud.com/2011/09/26/introducing-environments-run-anything-on-picloud/        
    * _fast_serialization:
        This keyword can be used to speed up serialization, at the cost of some functionality.
        This affects the serialization of the spawned jobs' return value.
        The stored function will always be serialized by the enhanced serializer, with debugging features.
        Possible values keyword are:
                    
        0. default -- use cloud module's enhanced serialization and debugging info            
        1. no debug -- Disable all debugging features for result            
        2. use cPickle -- Use python's fast serializer, possibly causing PicklingErrors                
    * _max_runtime:
        Specify the maximum amount of time (in integer minutes) a job can run. If job runs beyond 
        this time, it will be killed.
    * _priority: 
            A positive integer denoting the job's priority. PiCloud tries to run jobs 
            with lower priority numbers before jobs with higher priority numbers.                                              
    * _profile:
            Set this to True to enable profiling of your code. Profiling information is 
            valuable for debugging, but may slow down your jobs.
    * _restartable:
            In the very rare event of hardware failure, this flag indicates that a spawned 
            job can be restarted if the failure happened in the middle of it.
            By default, this is true. This should be unset if the function has external state
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
           
                 
    .. note:: 
    
        The callable must take no arguments.  To schedule functions that take arguments,
        wrap it with a closure. e.g. func(x,y) --> lambda: func(x,y)
    """
    
    if not callable(func):
        raise TypeError( 'cloud.cron.register first argument (%s) is not callable'  % (str(func) ))
    try:
        nargs = min_args(func)
    except TypeError: #some types we cannot error check
        nargs = 0         
    if nargs != 0:
        raise ValueError('cron functions must have 0 (required) arguments. %s requires %s' \
                         % (str(func), nargs))
        
        
    
    #TODO: Additional error checking for args
    
    #check for cron sanity
    if hasattr(schedule,'__iter__'):
        schedule = ' '.join(schedule)
    if not isinstance(schedule, str):
        schedule = str(schedule)
    CronTime(schedule) #throw exception if invalid cron format
    
    #ASCII label:
    try:
        label = label.decode('ascii').encode('ascii')
    except (UnicodeDecodeError, UnicodeEncodeError):
        raise TypeError('label must be an ASCII string')
    
    params = cloud.__cloud._getJobParameters(func,
                                             kwargs, 
                                             ignore=['_label', '_depends_on'])
    
    conn = cloud._getcloudnetconnection()
    
    sfunc, sarg, logprefix, logcnt = cloud.__cloud.adapter.cloud_serialize(func,
                                                                           params['fast_serialization'],
                                                                           [],
                                                                           logprefix='cron.')
    
    #Below is derived from HttpAdapter.job_add
    conn._update_params(params)
    
    cloud.__cloud.adapter.dep_snapshot() #let adapter make any needed calls for dep tracking
    
    data = Packer()
    data.add(sfunc)
    params['data'] = data.finish()
    
    params['label'] = label
    params['cron_exp'] = schedule                     
    
    conn.send_request(_register_query, params) 

def deregister(label):
    """Deregister (delete) the cron specified by *label*"""

    #ASCII label:
    try:
        label = label.decode('ascii').encode('ascii')
    except (UnicodeDecodeError, UnicodeEncodeError):
        raise TypeError('label must be an ASCII string')
    
    conn = cloud._getcloudnetconnection()
    
    conn.send_request(_deregister_query, {'label': label})   
        
def manual_run(label):
    """Manually run the cron specified by *label*
    Returns the 'jid' of the job created by this run command""" 

    #ASCII label:
    try:
        label = label.decode('ascii').encode('ascii')
    except (UnicodeDecodeError, UnicodeEncodeError):
        raise TypeError('label must be an ASCII string')
    
    conn = cloud._getcloudnetconnection()
    
    resp = conn.send_request(_run_query, {'label': label})
    return resp['jid']   
    
def list():
    """
    List labels of active crons
    """ 
    
    conn = cloud._getcloudnetconnection()
    resp = conn.send_request(_list_query, {})
    return resp['labels']

def info(label):
    """    
    Return a dictionary of relevant info about cron specified by *label*
    
    Info includes:
        
    * label: The same as was passed in
    * schedule: The schedule of this cron
    * last_run: Time this cron was last run
    * last_jid: Last run jid of this cron
    * created: When this cron was created   
    * creator_host: Hostname of the machine that registered this cron    
    * funcname: Name of function associated with cron
    """
    
    conn = cloud._getcloudnetconnection()
    resp = conn.send_request(_info_query, {'label':label})
    del resp['data']
    #del resp['version']
    return resp    
