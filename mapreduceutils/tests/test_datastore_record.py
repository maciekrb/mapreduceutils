import unittest
import datetime
from google.appengine.ext import db
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from mapreduceutils import MapperRecord

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

  def test_db_key_serialization(self):
    """ MapperRecord serializes db.Key objects """

    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, tests string",
      "expando_attr": "Some value here"
    }
    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 10)
    record = MapperRecord.create(SampleDbModel(key=key, **data))
    self.assertEqual(ndb.Key.from_old_key(key).urlsafe(), record.key)

  def test_ndb_key_serialization(self):
    """ MapperRecord serializes ndb.Key objects """

    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, tests string",
      "expando_attr": "Some value here"
    }

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 10)
    record = MapperRecord.create(SampleNDBModel(key=key, **data))
    self.assertEqual(key.urlsafe(), record.key)

  def test_instance_db_key_pair_resolution(self):
    """ MapperRecord resolves good key pairs for db.Model key attr """

    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, tests string",
      "expando_attr": "Some value here"
    }

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 10)
    record = MapperRecord.create(SampleDbModel(key=key, **data))
    expected = (('ABC', 1), ('BCD', 2), ('SampleDbModel', 10))
    self.assertEqual(expected, record._key_pairs)

  def test_instance_ndb_key_pair_resolution(self):
    """ MapperRecord resolves good key pairs for ndb.Model key attr """

    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, tests string",
      "expando_attr": "Some value here"
    }

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 10)
    record = MapperRecord.create(SampleNDBModel(key=key, **data))
    expected = (('ABC', 1), ('BCD', 2), ('SampleNDBModel', 10))
    self.assertEqual(expected, record._key_pairs)

  def test_db_attribute_resolution(self):
    """ MapperRecord resolves good values from db.Model objects """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 10)
    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, test string",
      "expando_attr": "Some value here"
    }
    record = MapperRecord.create(SampleDbModel(key=key, **data))

    self.assertEqual("test_record", record.record_type)
    exp_date = datetime.datetime(2010, 8, 12, 18, 23, 20)
    self.assertEqual(exp_date, record.created_time)
    self.assertEqual(3, record.data_quality)
    self.assertEqual("This is a, test string", record.schema_name)
    self.assertEqual("Some value here", record.expando_attr)

  def test_ndb_attribute_resolution(self):
    """ MapperRecord resolves good values from ndb.Model objects """

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 10)
    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, test string",
      "expando_attr": "Some value here"
    }
    record = MapperRecord.create(SampleNDBModel(key=key, **data))

    self.assertEqual("test_record", record.record_type)
    exp_date = datetime.datetime(2010, 8, 12, 18, 23, 20)
    self.assertEqual(exp_date, record.created_time)
    self.assertEqual(3, record.data_quality)
    self.assertEqual("This is a, test string", record.schema_name)
    self.assertEqual("Some value here", record.expando_attr)

  def test_ndb_structured_attribute_resolution(self):
    """ MapperRecord resolves values from structured properties  """

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 10)
    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, test string",
      "expando_attr": "Some value here",
      "ref_property": SampleNDBModel(**{
        "nested_property": 'ABC'
      })
    }

    record = MapperRecord.create(SampleNDBModel(key=key, **data))

    self.assertEqual("test_record", record.record_type)
    exp_date = datetime.datetime(2010, 8, 12, 18, 23, 20)
    self.assertEqual(exp_date, record.created_time)
    self.assertEqual(3, record.data_quality)
    props = record.pick_properties([
      'schema_name',
      'expando_attr',
      'ref_property',
      'ref_property.nested_property'
    ])
    self.assertEqual("This is a, test string", props['schema_name'])
    self.assertEqual("Some value here", props['expando_attr'])
    self.assertEqual(data['ref_property'], props['ref_property'])
    self.assertEqual(data['ref_property'].nested_property,
                     props['ref_property.nested_property'])

  def test_dict_attribute_resolution(self):
    """ MapperRecord resolves values from dict objects """

    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, test string",
      "expando_attr": "Some value here"
    }
    record = MapperRecord.create(data)

    props = record.pick_properties([
      'record_type',
      'created_time',
      'data_quality',
      'schema_name',
      'expando_attr'
    ])
    self.assertEqual("test_record", props['record_type'])
    exp_date = datetime.datetime(2010, 8, 12, 18, 23, 20)
    self.assertEqual(exp_date, props['created_time'])
    self.assertEqual(3, props['data_quality'])
    self.assertEqual("This is a, test string", props['schema_name'])
    self.assertEqual("Some value here", props['expando_attr'])


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
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, tests string",
      "expando_attr": "Some value here"
    }
    record = MapperRecord.create(SampleDbModel(key=key, **data))
    self.assertEqual(
      self.property_map[0],
      record.match_rule(self.property_map)
    )

  def test_rule_resolver_only_key(self):
    """ Rule resolver resolves rulset based solely on a key """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 10)
    data = {
      "record_type": "not_test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "aves",
      "expando_attr": "Some value here"
    }
    record = MapperRecord.create(SampleDbModel(key=key, **data))
    self.assertEqual(
      self.property_map[1],
      record.match_rule(self.property_map)
    )

  def test_rule_resolver_single_key_and_multiple_properties(self):
    """ Rule resolver resolves rulset based on a model key """

    key = db.Key.from_path('ABC', 1, 'BCD', 3, 'SampleDbModel', 10)
    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "aves",
      "expando_attr": "Some value here"
    }
    record = MapperRecord.create(SampleDbModel(key=key, **data))
    self.assertEqual(
      self.property_map[2],
      record.match_rule(self.property_map)
    )

  def test_rule_resolver_ndb_key_and_property(self):
    """ Rule resolver resolves ruleset based on a key and a property """

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 10)
    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, tests string",
      "expando_attr": "Some value here"
    }
    record = MapperRecord.create(SampleNDBModel(key=key, **data))
    self.assertEqual(
      self.property_map[0],
      record.match_rule(self.property_map)
    )


class TestDatastoreRecordFilters(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def test_db_key_pair_resolver(self):
    """ Key pairs are correctly resolved from from a db.Model """

    key = db.Key.from_path('ABC', 1, 'SampleDbModel', 2)
    record = MapperRecord.create(SampleDbModel(key=key))
    self.assertEqual((('ABC', 1), ('SampleDbModel', 2)), record._key_pairs)

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 2)
    record = MapperRecord.create(SampleDbModel(key=key))
    self.assertEqual(
      (('ABC', 1), ('BCD', 2), ('SampleDbModel', 2)),
      record._key_pairs
    )

  def test_ndb_key_pair_resolver(self):
    """ Key pairs are correctly resolved from from an ndb.Model """

    key = ndb.Key('ABC', 1, 'SampleNDBModel', 2)
    record = MapperRecord.create(SampleNDBModel(key=key))
    self.assertEqual((('ABC', 1), ('SampleNDBModel', 2)), record._key_pairs)

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 2)
    record = MapperRecord.create(SampleNDBModel(key=key))
    self.assertEqual(
      (('ABC', 1), ('BCD', 2), ('SampleNDBModel', 2)),
      record._key_pairs
    )

  def test_db_key_filters(self):
    """ Record key filtering in db.Model records works fine """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 3)
    record = MapperRecord.create(SampleDbModel(key=key, dummy="Dummy String"))

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

    res = record.matches_key_filters([
      (('ABC', 1), ('CDE', 2)),
      (('ABC', 1), ('BCD', 2))
    ])
    self.assertEqual(True, res)

    res = record.matches_key_filters([
      (('ABC', 1), ('BCD', 2)),
      (('ABC', 1), ('CDE', 2))
    ])
    self.assertEqual(True, res)

  def test_ndb_key_filters(self):
    """ Record key filtering in db.Model records works fine """

    key = ndb.Key('ABC', 1, 'BCD', 2, 'SampleNDBModel', 3)
    record = MapperRecord.create(SampleNDBModel(key=key, dummy="Dummy String"))

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

    res = record.matches_key_filters([
      (('ABC', 1), ('CDE', 2)),
      (('ABC', 1), ('BCD', 2))
    ])
    self.assertEqual(True, res)

    res = record.matches_key_filters([
      (('ABC', 1), ('BCD', 2)),
      (('ABC', 1), ('CDE', 2))
    ])
    self.assertEqual(True, res)

  def test_db_filters_match(self):
    """ Combinations of db.Model key_filters and property filters match """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleDbModel', 3)
    record = MapperRecord.create(SampleDbModel(key=key, a="test", b=123))

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
    record = MapperRecord.create(SampleNDBModel(key=key, a="test", b=123))

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
    record = MapperRecord.create(SampleNDBModel(key=key, a="test", b=123))

    """ one key match """
    pfilters = {}
    kfilters = [(('ABC', 1), ('BCD', 2)), (('ABC', 2), ('EJC', 3))]
    self.assertEqual(True, record.matches_filters(pfilters, kfilters))

    """ no key match  """
    pfilters = {}
    kfilters = [(('ABEC', 1), ('BCD', 2)), (('ABC', 2), ('EJC', 3))]
    self.assertEqual(False, record.matches_filters(pfilters, kfilters))

