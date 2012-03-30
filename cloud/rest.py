"""
Interface to publish functions to PiCloud, allowing them to be invoked via the REST API

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
import sys
import re

import cloud

from .util.zip_packer import Packer
from .util import getargspec

try:
    from json import dumps as serialize
    from json import loads as deserialize
except ImportError: #If python version < 2.6, we need to use simplejson
    from simplejson import dumps as serialize
    from simplejson import loads as deserialize

_publish_query = 'rest/register/'
_remove_query = 'rest/deregister/'
_list_query = 'rest/list/'
_info_query = 'rest/info/'

"""
This module utilizes the cloud object extensively
The functions can be viewed as instance methods of the Cloud (hence accessing of protected variables)
"""

   
def publish(func, label, out_encoding='json', **kwargs):
    """       
    Publish *func* (a callable) to PiCloud so it can be invoked through the PiCloud REST API
    
    The published function will be managed in the future by a unique (URL encoded) *label*. 
    
    *out_encoding* specifies the format that the return value should be in when retrieving the result
    via the REST API. Valid values are "json" for a JSON-encoded object and "raw", where the return value
    must be an str (but can contain any characters).
    
    The return value is the URL which can be HTTP POSTed to to invoke *func*. 
    See http://docs.picloud.com/rest.html for information about PiCloud's REST API    
    
    Certain special *kwargs* associated with cloud.call can be attached to the periodic jobs: 
        
    * _fast_serialization:
        This keyword can be used to speed up serialization, at the cost of some functionality.
        This affects the serialization of the spawned jobs' return value.
        The stored function will always be serialized by the enhanced serializer, with debugging features.
        Possible values keyword are:
                    
        0. default -- use cloud module's enhanced serialization and debugging info            
        1. no debug -- Disable all debugging features for result            
        2. use cPickle -- Use python's fast serializer, possibly causing PicklingErrors                

    * _env:
        A string specifying a custom environment you wish to run your generated jobs within.
        See environments overview at 
        http://blog.picloud.com/2011/09/26/introducing-environments-run-anything-on-picloud/
    * _priority: 
            A positive integer denoting the job's priority. PiCloud tries to run jobs 
            with lower priority numbers before jobs with higher priority numbers.                        
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

    """
    
    if not callable(func):
        raise TypeError( 'cloud.rest.publish first argument (%s) is not callable'  % (str(func) ))        
    
    m = re.match(r'^[A-Z0-9a-z_+-.]+$', label)
    if not m:
        raise TypeError('Label can only consist of valid URI characters (alphanumeric or from set(_+-.$)')
        
    #ASCII label:
    try:
        label = label.decode('ascii').encode('ascii')
    except (UnicodeDecodeError, UnicodeEncodeError): #should not be possible
        raise TypeError('label must be an ASCII string')
    
    try:
        docstring = '' if (func.__doc__ is None) else func.__doc__
        func_desc = (docstring).encode('utf8')
    except (UnicodeDecodeError, UnicodeEncodeError):
        raise TypeError('function docstring must be an UTF8 compatible unicode string')
    
    if not isinstance(out_encoding, str):
        raise TypeError('out_encoding must be an ASCII string')
    
    
    
    params = cloud.__cloud._getJobParameters(func,
                                             kwargs, 
                                             ignore=['_label', '_depends_on'])
    
    #argument specification for error checking and visibility
    argspec = getargspec(func)    
    argspec_serialized = serialize(argspec,default=str)
    if len(argspec_serialized) >= 255: #won't fit in db - clear defaults
        argspec[4] = {}
        argspec_serialized = serialize(argspec,default=str)        
    params['argspec'] = argspec_serialized
    
    conn = cloud._getcloudnetconnection()
    
    sfunc, sarg, logprefix, logcnt = cloud.__cloud.adapter.cloud_serialize(func,
                                                                           params['fast_serialization'],
                                                                           [],
                                                                           logprefix='rest.')
    
    #Below is derived from HttpAdapter.job_add
    conn._update_params(params)
    
    cloud.__cloud.adapter.dep_snapshot() #let adapter make any needed calls for dep tracking
    
    data = Packer()
    data.add(sfunc)
    
    params['data'] = data.finish()
    params['label'] = label
    params['description'] = func_desc
    params['out_encoding'] = out_encoding
    
    resp = conn.send_request(_publish_query, params)
    
    return resp['uri']

register = publish 

def remove(label):
    """
    Remove a published function from PiCloud
    """

    #ASCII label:
    try:
        label = label.decode('ascii').encode('ascii')
    except (UnicodeDecodeError, UnicodeEncodeError):
        raise TypeError('label must be an ASCII string')
    
    conn = cloud._getcloudnetconnection()
    
    conn.send_request(_remove_query, {'label': label})   
    
deregister = remove    
   
def list():
    """
    List labels of published functions
    """ 
    #note beware: List is defined as this function
    conn = cloud._getcloudnetconnection()
    resp = conn.send_request(_list_query, {})
    return resp['labels']

def info(label):
    """
    Retrieve information about a published function specified by *label*
    """
    conn = cloud._getcloudnetconnection()
    resp = conn.send_request(_info_query, {'label':label})
    del resp['data']
    
    return resp
