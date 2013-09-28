import unittest
import datetime
from google.appengine.ext import db
from google.appengine.ext import testbed
from datastoreutils import _get_ruleset

class SampleModel(db.Expando):
  pass

class TestRecordFilters(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

    self.property_map = [
      { "model_match_rule": {
          "key" : [("ABC", 1), ("BCD", 2)],
          "properties" : [("record_type", "test_record")]
        }
      },
      { "model_match_rule": {
          "key" : [("ABC", 1), ("BCD", 2)]
        }
      },
      { "model_match_rule": {
          "properties" : [("record_type", "test_record"), ("schema_name", "aves")]
        }
      },
      { "model_match_rule": {
          "properties" : [("record_type", "test_record")]
        }
      }
    ]

  def tearDown(self):
    self.testbed.deactivate()
    pass

  def test_rule_resolver_key_and_property(self):
    """ Rule resolver resolves ruleset based on a key and a property """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleModel', 10)
    data = {
      "record_type" : "test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "This is a, tests string",
      "expando_attr": "Some value here"
    }
    record = SampleModel(key=key, **data)
    ruleset = _get_ruleset(self.property_map, record)
    self.assertEqual(self.property_map[0], ruleset)

  def test_rule_resolver_only_key(self):
    """ Rule resolver resolves rulset based solely on a key """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleModel', 10)
    data = {
      "record_type" : "not_test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "aves",
      "expando_attr": "Some value here"
    }
    record = SampleModel(key=key, **data)
    ruleset = _get_ruleset(self.property_map, record)
    self.assertEqual(self.property_map[1], ruleset)

  def test_rule_resolver_single_key_and_multiple_properties(self):
    """ Rule resolver resolves rulset based on a model key """

    key = db.Key.from_path('ABC', 1, 'BCD', 3, 'SampleModel', 10)
    data = {
      "record_type" : "test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "aves",
      "expando_attr": "Some value here"
    }
    record = SampleModel(key=key, **data)
    ruleset = _get_ruleset(self.property_map, record)
    self.assertEqual(self.property_map[2], ruleset)


