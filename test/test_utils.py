import sys
import unittest


class DatastoreTest(unittest.TestCase):
	def setUp(self):
		from google.appengine.ext import testbed
		from google.appengine.datastore import datastore_stub_util
		self.testbed = testbed.Testbed()
		self.testbed.activate()
		self.policy = datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=0)
		self.testbed.init_datastore_v3_stub(consistency_policy=self.policy)
		self.testbed.init_memcache_stub()

	def tearDown(self):
		self.testbed.deactivate()


SDK_PATH = '/Applications/GoogleAppEngineLauncher.app/Contents/Resources/GoogleAppEngine-default.bundle/Contents/Resources/google_appengine'


def setup():
	sys.path.insert(0, SDK_PATH)
	import dev_appserver
	dev_appserver.fix_sys_path()
