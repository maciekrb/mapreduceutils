import unittest
import datetime
from google.appengine.ext import db
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from datastoreutils import DatastoreRecord

class SampleDbModel(db.Expando):
  pass

class SampleNDBModel(ndb.Expando):
  pass

class TestDatastoreRecordAttributeResolution(unittest.TestCase):
  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def test_instance_key_resolution(self):
    """ DatastoreRecord resolves good ndb.Key for db.Model and ndb.Model objects """
    data = {
      "record_type" : "test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "This is a, tests string",
      "expando_attr": "Some value here"
    }
    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 10)
    record = DatastoreRecord(SampleDbModel(key=key, **data))
    self.assertEqual(ndb.Key.from_old_key(key), record.key)

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 10)
    record = DatastoreRecord(SampleNDBModel(key=key, **data))
    self.assertEqual(key, record.key)

  def test_instance_key_pair_resolution(self):
    """ DatastoreRecord resolves good key pairs for db.Model and ndb.Model objects """

    data = {
      "record_type" : "test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "This is a, tests string",
      "expando_attr": "Some value here"
    }

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 10)
    record = DatastoreRecord(SampleDbModel(key=key, **data))
    self.assertEqual((('ABC', 1), ('BCD', 2), ('SampleDbModel', 10)), record.key_pairs)

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 10)
    record = DatastoreRecord(SampleNDBModel(key=key, **data))
    self.assertEqual((('ABC', 1), ('BCD', 2), ('SampleNDBModel', 10)), record.key_pairs)

  def test_db_attribute_resolution(self):
    """ DatastoreRecord resolves good values from db.Model objects """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 10)
    data = {
      "record_type" : "test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "This is a, test string",
      "expando_attr": "Some value here"
    }
    record = DatastoreRecord(SampleDbModel(key=key, **data))

    self.assertEqual("test_record", record.record_type)
    self.assertEqual(datetime.datetime(2010,8,12,18,23,20), record.created_time)
    self.assertEqual(3, record.data_quality)
    self.assertEqual("This is a, test string", record.schema_name)
    self.assertEqual("Some value here", record.expando_attr)

  def test_ndb_attribute_resolution(self):
    """ DatastoreRecord resolves good values from ndb.Model objects """

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 10)
    data = {
      "record_type" : "test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "This is a, test string",
      "expando_attr": "Some value here"
    }
    record = DatastoreRecord(SampleNDBModel(key=key, **data))

    self.assertEqual("test_record", record.record_type)
    self.assertEqual(datetime.datetime(2010,8,12,18,23,20), record.created_time)
    self.assertEqual(3, record.data_quality)
    self.assertEqual("This is a, test string", record.schema_name)
    self.assertEqual("Some value here", record.expando_attr)


class TestDatastoreRecordFilterMatching(unittest.TestCase):

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

  def test_rule_resolver_db_key_and_property(self):
    """ Rule resolver resolves ruleset based on a db.Key and a property """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 10)
    data = {
      "record_type" : "test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "This is a, tests string",
      "expando_attr": "Some value here"
    }
    record = DatastoreRecord(SampleDbModel(key=key, **data))
    self.assertEqual(self.property_map[0], record.match_rule(self.property_map))

  def test_rule_resolver_only_key(self):
    """ Rule resolver resolves rulset based solely on a key """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 10)
    data = {
      "record_type" : "not_test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "aves",
      "expando_attr": "Some value here"
    }
    record = DatastoreRecord(SampleDbModel(key=key, **data))
    self.assertEqual(self.property_map[1], record.match_rule(self.property_map))

  def test_rule_resolver_single_key_and_multiple_properties(self):
    """ Rule resolver resolves rulset based on a model key """

    key = db.Key.from_path('ABC', 1, 'BCD', 3, 'SampleDbModel', 10)
    data = {
      "record_type" : "test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "aves",
      "expando_attr": "Some value here"
    }
    record = DatastoreRecord(SampleDbModel(key=key, **data))
    self.assertEqual(self.property_map[2], record.match_rule(self.property_map))

  def test_rule_resolver_ndb_key_and_property(self):
    """ Rule resolver resolves ruleset based on a key and a property """

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 10)
    data = {
      "record_type" : "test_record",
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality" : 3,
      "schema_name" : "This is a, tests string",
      "expando_attr": "Some value here"
    }
    record = DatastoreRecord(SampleNDBModel(key=key, **data))
    self.assertEqual(self.property_map[0], record.match_rule(self.property_map))


class TestDatastoreRecordFilters(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()


  def test_key_pair_resolver(self):
    """ Key pairs are correctly resolved from from a db.Model and ndb.Model object """

    key = db.Key.from_path('ABC', 1, 'SampleDbModel', 2)
    record = DatastoreRecord(SampleDbModel(key=key))
    self.assertEqual((('ABC', 1), ('SampleDbModel', 2)), record.key_pairs)

    key = ndb.Key('ABC', 1, 'SampleNDBModel', 2)
    record = DatastoreRecord(SampleNDBModel(key=key))
    self.assertEqual((('ABC', 1), ('SampleNDBModel', 2)), record.key_pairs)

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 2)
    record = DatastoreRecord(SampleDbModel(key=key))
    self.assertEqual((('ABC', 1), ('BCD', 2), ('SampleDbModel', 2)), record.key_pairs)

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 2)
    record = DatastoreRecord(SampleNDBModel(key=key))
    self.assertEqual((('ABC', 1), ('BCD', 2), ('SampleNDBModel', 2)), record.key_pairs)

  def test_db_key_filters(self):
    """ Record key filtering in db.Model records works fine """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 3)
    record = DatastoreRecord(SampleDbModel(key=key, dummy="Dummy String"))

    res = record.matches_key_filters(None)
    self.assertEqual(True, res)

    res = record.matches_key_filters([(('ABC', 1),)])
    self.assertEqual(True, res)

    res = record.matches_key_filters([(('ABC', 1), ('BCD', 2))])
    self.assertEqual(True, res)

    res = record.matches_key_filters([(('ABC', 2), ('BCD', 2))])
    self.assertEqual(False, res)

    res = record.matches_key_filters([(('ABC', 1), ('CDE', 2))])
    self.assertEqual(False, res)

    res = record.matches_key_filters([(('ABC', 1), ('CDE', 2)), (('ABC', 1), ('BCD', 2))])
    self.assertEqual(True, res)

    res = record.matches_key_filters([(('ABC', 1), ('BCD', 2)), (('ABC', 1), ('CDE', 2))])
    self.assertEqual(True, res)

  def test_ndb_key_filters(self):
    """ Record key filtering in db.Model records works fine """

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 3)
    record = DatastoreRecord(SampleNDBModel(key=key, dummy="Dummy String"))

    res = record.matches_key_filters(None)
    self.assertEqual(True, res)

    res = record.matches_key_filters([(('ABC', 1),)])
    self.assertEqual(True, res)

    res = record.matches_key_filters([(('ABC', 1), ('BCD', 2))])
    self.assertEqual(True, res)

    res = record.matches_key_filters([(('ABC', 2), ('BCD', 2))])
    self.assertEqual(False, res)

    res = record.matches_key_filters([(('ABC', 1), ('CDE', 2))])
    self.assertEqual(False, res)

    res = record.matches_key_filters([(('ABC', 1), ('CDE', 2)), (('ABC', 1), ('BCD', 2))])
    self.assertEqual(True, res)

    res = record.matches_key_filters([(('ABC', 1), ('BCD', 2)), (('ABC', 1), ('CDE', 2))])
    self.assertEqual(True, res)

  def test_db_filters_match(self):
    """ Combinations of db.Model key_filters and property filters match """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 3)
    record = DatastoreRecord(SampleDbModel(key=key, a="test", b=123))

    """ key only filter success """
    pfilters = {}
    kfilters = [(('ABC', 1), ('BCD', 2))]
    self.assertEqual(True, record.matches_filters(pfilters, kfilters))

    """ key only filter fail """
    pfilters = {}
    kfilters = [(('ABC', 1), ('BCE', 2))]
    self.assertEqual(False, record.matches_filters(pfilters, kfilters))

    """ key filter with property success """
    pfilters = [('a', '=', 'test')]
    kfilters = [(('ABC', 1), ('BCD', 2))]
    self.assertEqual(True, record.matches_filters(pfilters, kfilters))

    """ key filter with property fail """
    pfilters = [('a', '=', 'badtest')]
    kfilters = [(('ABC', 1), ('BCD', 2))]
    self.assertEqual(False, record.matches_filters(pfilters, kfilters))

    """ key filter failure with good property filter """
    pfilters = [('a', '=', 'test')]
    kfilters = [(('ABC', 1), ('BCD', 3))]
    self.assertEqual(False, record.matches_filters(pfilters, kfilters))


  def test_ndb_filters_match(self):
    """ Combinations of ndb.Model key_filters and property filters match """

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 3)
    record = DatastoreRecord(SampleNDBModel(key=key, a="test", b=123))

    """ key only filter success """
    pfilters = {}
    kfilters = [(('ABC', 1), ('BCD', 2))]
    self.assertEqual(True, record.matches_filters(pfilters, kfilters))

    """ key only filter fail """
    pfilters = {}
    kfilters = [(('ABC', 1), ('BCE', 2))]
    self.assertEqual(False, record.matches_filters(pfilters, kfilters))


    """  key filter with property success """
    pfilters = [('a', '=', 'test')]
    kfilters = [(('ABC', 1), ('BCD', 2))]
    self.assertEqual(True, record.matches_filters(pfilters, kfilters))

    """ key filter with property fail """
    pfilters = [('a', '=', 'badtest')]
    kfilters = [(('ABC', 1), ('BCD', 2))]
    self.assertEqual(False, record.matches_filters(pfilters, kfilters))

    """ key filter failure with good property filter """
    pfilters = [('a', '=', 'test')]
    kfilters = [(('ABC', 1), ('BCD', 3))]
    self.assertEqual(False, record.matches_filters(pfilters, kfilters))

  def test_filters_OR_match(self):
    """ ModelRuleSet filters OR logic works """

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 3)
    record = DatastoreRecord(SampleNDBModel(key=key, a="test", b=123))

    """ one key match """
    pfilters = {}
    kfilters = [(('ABC', 1), ('BCD', 2)), (('ABC', 2), ('EJC', 3))]
    self.assertEqual(True, record.matches_filters(pfilters, kfilters))

    """ no key match  """
    pfilters = {}
    kfilters = [(('ABEC', 1), ('BCD', 2)), (('ABC', 2), ('EJC', 3))]
    self.assertEqual(False, record.matches_filters(pfilters, kfilters))

