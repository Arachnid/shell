#!/usr/bin/python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
An interactive, stateful AJAX shell that runs Python code on the server.

Part of http://code.google.com/p/google-app-engine-samples/.

May be run as a standalone app or in an existing app as an admin-only handler.
Can be used for system administration tasks, as an interactive way to try out
APIs, or as a debugging aid during development.

The logging, os, sys, db, and users modules are imported automatically.

Interpreter state is stored in the datastore so that variables, function
definitions, and other values in the global and local namespaces can be used
across commands.

To use the shell in your app, copy shell.py, static/*, and templates/* into
your app's source directory. Then, copy the URL handlers from app.yaml into
your app.yaml.

TODO: unit tests!
"""

import cPickle
import cStringIO
import json
import logging
import new
import os
import pickle
import pydoc
import sys
import traceback
import types
import wsgiref.handlers

from google.appengine.api import users
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template

from cloud.serialization import cloudpickle


# Set to True if stack traces should be shown in the browser, etc.
_DEBUG = True

# The entity kind for shell sessions. Feel free to rename to suit your app.
_SESSION_KIND = '_Shell_Session'

# Types that can't be pickled.
UNPICKLABLE_TYPES = (
  types.ModuleType,
  types.TypeType,
  types.ClassType,
  types.FunctionType,
  )

# Unpicklable statements to seed new sessions with.
INITIAL_UNPICKLABLES = [
  'import logging',
  'import os',
  'import sys',
  'from google.appengine.ext import db',
  'from google.appengine.api import users',
  'class Foo(db.Expando):\n  pass',
  ]


def dumps(obj):
  s = cStringIO.StringIO()
  pickler = cloudpickle.CloudPickler(s)
  pickler.dump(obj)
  return s.getvalue()

loads = cPickle.loads


class Session(db.Model):
  """A shell session. Stores the session's globals.

  Each session globals is stored in one of two places:

  If the global is picklable, it's stored in the parallel globals and
  global_names list properties. (They're parallel lists to work around the
  unfortunate fact that the datastore can't store dictionaries natively.)

  If the global is not picklable (e.g. modules, classes, and functions), or if
  it was created by the same statement that created an unpicklable global,
  it's not stored directly. Instead, the statement is stored in the
  unpicklables list property. On each request, before executing the current
  statement, the unpicklable statements are evaluated to recreate the
  unpicklable globals.

  The unpicklable_names property stores all of the names of globals that were
  added by unpicklable statements. When we pickle and store the globals after
  executing a statement, we skip the ones in unpicklable_names.

  Using Text instead of string is an optimization. We don't query on any of
  these properties, so they don't need to be indexed.
  """
  state = db.BlobProperty()

  @classmethod
  def create(cls):
    session = cls()
    with session.activate() as context:
      for line in INITIAL_UNPICKLABLES:
        context.execute(line)
    return session

  def activate(self, stdout=None, stderr=None):
    return SessionContext(self, stdout, stderr)


class SessionContext(object):
  """Encapsulates a session, and provides ways to execute code inside it."""

  def __init__(self, session, stdout, stderr):
    self.session = session
    self.stdout = stdout
    self.stderr = stderr

  def __enter__(self):
    """Deserializes and invokes this session context, substituting it for __main__.
    
    Returns: The module containing the reconstituted session.
    """
    if self.session.state:
      # deserialize the module's dict
      self.module = loads(self.session.state)
    else:
      # create a dedicated module to be used for the session's __main__.
      self.module = new.module('__main__')

    # use the current __builtin__, since it changes on each request.
    # this is needed for import statements, among other things.
    import __builtin__
    self.module.__builtins__ = __builtin__

    # swap in our custom module for __main__. then unpickle the session
    # globals inside it.
    self._old_main = sys.modules.get('__main__')
    self._old_stdout = sys.stdout
    self._old_stderr = sys.stderr
    try:
      sys.modules['__main__'] = self.module
      if self.stdout:
        sys.stdout = self.stdout
      if self.stderr:
        sys.stderr = self.stderr
      
      self.module.__name__ = '__main__'

      # run!
      return self
    except Exception:
      self._cleanup()
      raise

  def execute(self, statement):
    self.statement = statement
    compiled = compile(statement, '<string>', 'single')
    exec compiled in self.module.__dict__

  def __exit__(self, exc_type, exc_value, traceback):
    try:
      self.session.state = dumps(self.module)
    finally:
      self._cleanup()

  def _cleanup(self):
    sys.stdout = self._old_stdout
    sys.stderr = self._old_stderr
    sys.modules['__main__'] = self._old_main
    

class FrontPageHandler(webapp.RequestHandler):
  """Creates a new session and renders the shell.html template.
  """

  def get(self):
    # set up the session. TODO: garbage collect old shell sessions
    session_key = self.request.get('session')
    if session_key:
      session = Session.get(session_key)
    else:
      # create a new session
      session = Session.create()
      session_key = session.put()

    template_file = os.path.join(os.path.dirname(__file__), 'templates',
                                 'shell.html')
    session_url = '/?session=%s' % session_key
    vars = { 'server_software': os.environ['SERVER_SOFTWARE'],
             'python_version': sys.version,
             'session': str(session_key),
             'user': users.get_current_user(),
             'login_url': users.create_login_url(session_url),
             'logout_url': users.create_logout_url(session_url),
             }
    rendered = template.render(template_file, vars, debug=_DEBUG)
    self.response.out.write(unicode(rendered))


class StatementHandler(webapp.RequestHandler):
  """Evaluates a python statement in a given session and returns the result.
  """
  
  def write_json(self, result, symbols=None):
    if symbols is None: symbols = {}
    self.response.out.write(json.dumps({
      'result': result,
      'symbols': symbols,
    }))

  def generate_docstrings(self, module):
    doc = pydoc.HTMLDoc()
    return dict((k, doc.document(v)) for k, v in module.__dict__.iteritems() if not isinstance(v, types.ModuleType))

  def get(self):
    self.response.headers['Content-Type'] = 'application/json'

    # extract the statement to be run
    statement = self.request.get('statement')
    if not statement:
      return

    # the python compiler doesn't like network line endings
    statement = statement.replace('\r\n', '\n')

    # add a couple newlines at the end of the statement. this makes
    # single-line expressions such as 'class Foo: pass' evaluate happily.
    statement += '\n\n'

    # load the session from the datastore
    session = Session.get(self.request.get('session'))

    output = cStringIO.StringIO()
    with session.activate(output, output) as context:
      try:
        context.execute(statement)
        self.write_json(output.getvalue(), self.generate_docstrings(context.module))
      except:
        self.write_json(traceback.format_exc())
        return

    session.put()


def main():
  application = webapp.WSGIApplication(
    [('/', FrontPageHandler),
     ('/shell.do', StatementHandler)], debug=_DEBUG)
  wsgiref.handlers.CGIHandler().run(application)


if __name__ == '__main__':
  main()
