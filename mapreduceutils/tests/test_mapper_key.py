from mapreduceutils import MapperRecord
import datetime
from google.appengine.ext import (
  ndb,
  testbed
)
import unittest


class DummyModel(ndb.Expando):
  pass


class TestModelMapperKey(unittest.TestCase):
  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def test_mapper_key_parses_single_attr(self):
    """ MapperRecord parses single attribute to generate a mapper key """
    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, tests string",
      "expando_attr": "Some value here"
    }

    mapper_key_spec = [
      'created_time'
    ]

    entity = DummyModel(**data)
    entity.put()
    record = MapperRecord.create(entity)
    self.assertEqual(
      '2010-08-12 18:23:20',
      record.mapper_key(mapper_key_spec)
    )

  def test_mapper_key_parses_composite_attr(self):
    """ MapperRecord parses composite attributes to generate a mapper key """
    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, tests string",
      "expando_attr": "Some value here"
    }

    mapper_key_spec = [
      'created_time',
      'data_quality'
    ]

    entity = DummyModel(**data)
    entity.put()
    record = MapperRecord.create(entity)
    self.assertEqual(
      '2010-08-12 18:23:20|3',
      record.mapper_key(mapper_key_spec)
    )

  def test_mapper_key_parses_modifiers(self):
    """ MapperRecord parses modifiers to generate a mapper key """
    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, tests string",
      "expando_attr": "Some value here"
    }

    mapper_key_spec = [
      [{
        "method": "mapreduceutils.modifiers.primitives.DateFormatModifier",
        "identifier": "chaining_id",
        "operands": {"value": "model.created_time"},
        "args": {"date_format": "%Y"}
      }]
    ]

    entity = DummyModel(**data)
    entity.put()
    record = MapperRecord.create(entity)
    self.assertEqual(
      '2010',
      record.mapper_key(mapper_key_spec)
    )

  def test_mapper_key_parses_chained_modifiers(self):
    """ MapperRecord parses chained modifiers to generate a mapper key """
    data = {
      "record_type": "test_record",
      "created_time": datetime.datetime(2010, 8, 12, 18, 23, 20),
      "data_quality": 3,
      "schema_name": "This is a, tests string",
      "expando_attr": "Some value here"
    }

    mapper_key_spec = [
      'record_type',
      [{
        "method": "mapreduceutils.modifiers.primitives.DateFormatModifier",
        "identifier": "chaining_id",
        "operands": {"value": "model.created_time"},
        "args": {"date_format": "%Y"}
      }],
      [{
        "method": "mapreduceutils.modifiers.primitives.DateFormatModifier",
        "identifier": "other_chaining_id",
        "operands": {"value": "model.created_time"},
        "args": {"date_format": "%m"}
      }]
    ]

    entity = DummyModel(**data)
    entity.put()
    record = MapperRecord.create(entity)
    self.assertEqual(
      'test_record|2010|08',
      record.mapper_key(mapper_key_spec)
    )
