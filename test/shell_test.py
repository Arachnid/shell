from test import test_utils
test_utils.setup()

import unittest
import urllib
from google.appengine.ext import webapp
from google.appengine.ext import db

import shell


class ShellTest(test_utils.DatastoreTest):
  def setUp(self):
    super(ShellTest, self).setUp()
    self.session = shell.Session()
    self.session.unpicklables = [db.Text(line) for line in shell.INITIAL_UNPICKLABLES]
    self.session.put()

  def execute_statement(self, statement):
    environ = {
        'QUERY_STRING': urllib.urlencode({
            'session': str(self.session.key()),
            'statement': statement,
        }),
    }
    request = webapp.Request(environ)
    response = webapp.Response()
    handler = shell.StatementHandler()
    handler.initialize(request, response)
    handler.get()
    return response.out.getvalue()

  def testSimpleStatements(self):
    self.assertEqual(self.execute_statement("print 5"), "5\n")
    self.assertEqual(self.execute_statement("2 + 2"), "4\n")

  def testPersistence(self):
    self.assertEqual(self.execute_statement("def add(a, b): return a+b"), "")
    self.assertEqual(self.execute_statement("add(2,4)"), "6\n")
    session = db.get(self.session.key())

  def testModifyGlobals(self):
    self.assertEqual(self.execute_statement("foo = 3"), "")
    self.assertEqual(self.execute_statement("foo"), "3\n")
    self.assertEqual(self.execute_statement("foo = 4"), "")
    self.assertEqual(self.execute_statement("foo"), "4\n")

  def testMutableValues(self):
    self.assertEqual(self.execute_statement("foo = {}"), "")
    self.assertEqual(self.execute_statement("foo['bar'] = 3"), "")
    self.assertEqual(self.execute_statement("foo"), "{'bar': 3}\n")


if __name__ == '__main__':
  unittest.main()
