#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import logging
import datetime
from testlib import testutil
from google.appengine.ext import db
from google.appengine.api import files
from google.appengine.api.files import records
from google.appengine.ext import testbed
from mapreduce import mapreduce_pipeline
from mapreduce import input_readers
from mapreduce import test_support
from mapreduce.lib import pipeline
from datastoreutils import record_map

def _run_pipeline(taskqueue, entity_kind, mapper_function, params={}):
  """
  Runs mapper pipeline with given params and returns a list
  containing data resulting from mapper_function

  Args:
    - entity_kind: (str) sting containing the module path of the Datastore Model to read
    - mapper_function: (str) sting containing the module path of the mapper function

  Returns:
    - List with records resulting from the pipeline processing
  """

  params["input_reader"] = { "entity_kind": entity_kind }

  # Run Mapreduce
  p = mapreduce_pipeline.MapperPipeline(
    "Datastore Mapper",
    mapper_function,
    input_readers.__name__ + ".DatastoreInputReader",
    output_writer_spec="mapreduce.output_writers.BlobstoreRecordsOutputWriter",
    params=params,
    shards=10)
  p.start()
  test_support.execute_until_empty(taskqueue)

  p = mapreduce_pipeline.MapperPipeline.from_id(p.pipeline_id)
  output_data = []
  for output_file in p.outputs.default.value:
    with files.open(output_file, "r") as f:
      for record in records.RecordsReader(f):
        output_data.append(record)

  return output_data

class DummyReference(db.Expando):
  pass

class TestRecord(db.Expando):
  record_entry = db.ReferenceProperty(reference_class=DummyReference, collection_name="records")
  tag_keys = db.ListProperty(db.Key)
  created_time = db.DateTimeProperty()
  data_quality = db.IntegerProperty(default=1)
  schema = db.ReferenceProperty(reference_class=DummyReference, collection_name="schema_records")
  schema_name = db.StringProperty()


class DatastoreOutput(testutil.HandlerTestBase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()

    testutil.HandlerTestBase.setUp(self)
    pipeline.Pipeline._send_mail = self._send_mail
    self.emails = []

  def _send_mail(self, sender, subject, body, html=None):
    self.emails.append((sender, subject, body, html))

  def tearDown(self):
    self.testbed.deactivate()

  def test_map_pipeline_csv_conversion(self):
    """ Tests all records were dumped and that CSV format is consistent """

    # Prepare test data
    test_data = [
      {
        "record_type" : "test_record",
        "created_time" : datetime.datetime(2010,8,12,18,23,20),
        "data_quality" : 3,
        "schema_name" : "This is a, tests string",
        "expando_attr": "Some value here"
      },
      {
        "record_type" : "test_record",
        "created_time" : datetime.datetime(2011,9,11,19,20,21),
        "data_quality" : 4,
        "schema_name" : "some_schema_name",
        "expando_attr": "Some other value here"
      }
    ]

    for r in test_data:
      TestRecord(**r).put()

    output_data = _run_pipeline(
      self.taskqueue,
      __name__ + ".TestRecord",
      __name__ + ".record_map",
      params={
        "property_map": [{
          "property_match_name": "record_type",
          "property_match_value": "test_record",
          "property_list": [
            "created_time",
            "data_quality",
            "expando_attr",
            "schema_name"
          ]
        }]
      }
    )

    # Asset Pipeline finished
    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith("Pipeline successful:"))

    # Assert number of rows, and format of one of them. Order is not predictable
    self.assertEquals(2, len(output_data))
    rec = output_data[0] if output_data[0].startswith('2010-') else output_data[1]
    self.assertEquals('2010-08-12 18:23:20,3,Some value here,"This is a, tests string"\r\n', rec)

  def test_map_pipeline_property_map(self):
    """ Test that only mapped properties are included in the resulting record """

    record_data = {
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality": 1,
      "schema_name": u"cobertura_vegetal",
      "codigo": 3131,
      "nombre": u"ALTO FRAGUA INDIWASI",
      "nivel_1": u"Bosques y Áreas Seminaturales",
      "nivel_2": u"Bosques",
      "nivel_3": u"Bosque fragmentado",
      "leyenda": u"Bosque fragmentado con pastos y cultivos",
      "codigo_1": 3131,
      "nombre_1": u"ALTO FRAGUA INDIWASI",
      "interpretacion": 2,
      "nivel_11": u"Bosques y Áreas Seminaturales",
      "nivel_12": u"Bosques",
      "nivel_13": u"Bosque fragmentado",
      "leyenda_1": u"Bosque fragmentado con pastos y cultivos",
      "hectareas": 8.283567
    }
    TestRecord(**record_data).put()

    output_data = _run_pipeline(
      self.taskqueue,
      __name__ + ".TestRecord",
      __name__ + ".record_map",
      params={
        "property_map": [{
          "property_match_name": "schema_name",
          "property_match_value": "cobertura_vegetal",
          "property_list": [
            "created_time",
            "schema_name",
            "nivel_1",
            "data_quality"
          ]
        }]
      }
    )

    # Asset Pipeline finished
    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith("Pipeline successful:"))

    # Assert number of rows, and format of one of them. Order is not predictable
    self.assertEquals(1, len(output_data))
    rec = '2010-08-12 18:23:20,cobertura_vegetal,Bosques y Áreas Seminaturales,1\r\n'
    self.assertEquals(rec, output_data[0])

  def test_map_pipeline_reference_property_mapping(self):
    """ Test that ReferenceProperties can be used as property_match_values """

    record_entry = DummyReference(context="some_context").put()
    schema  = DummyReference(name="cobertura_vegetal").put()
    tag_ABC = DummyReference(name="ABC").put()
    tag_BCD = DummyReference(name="BCD").put()

    record_data = {
      "record_entry": record_entry,
      "tag_keys": [tag_ABC, tag_BCD],
      "created_time" : datetime.datetime(2010,8,12,18,23,20),
      "data_quality": 1,
      "schema": schema,
      "schema_name": u"cobertura_vegetal",
      "codigo": 3131,
      "nombre": u"ALTO FRAGUA INDIWASI",
      "nivel_1": u"Bosques y Áreas Seminaturales",
      "nivel_2": u"Bosques",
      "nivel_3": u"Bosque fragmentado",
      "leyenda": u"Bosque fragmentado con pastos y cultivos",
      "codigo_1": 3131,
      "nombre_1": u"ALTO FRAGUA INDIWASI",
      "interpretacion": 2,
      "nivel_11": u"Bosques y Áreas Seminaturales",
      "nivel_12": u"Bosques",
      "nivel_13": u"Bosque fragmentado",
      "leyenda_1": u"Bosque fragmentado con pastos y cultivos",
      "hectareas": 8.283567
    }
    TestRecord(**record_data).put()

    output_data = _run_pipeline(
      self.taskqueue,
      __name__ + ".TestRecord",
      __name__ + ".record_map",
      params={
        "property_map": [{
          "property_match_name": "schema",
          "property_match_value": str(schema),
          "property_list": [
            "created_time",
            "schema_name",
            "nivel_1",
            "data_quality"
          ]
        }]
      }
    )

    # Asset Pipeline finished
    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith("Pipeline successful:"))

    # Assert number of rows, and format of one of them. Order is not predictable
    self.assertEquals(1, len(output_data))
    rec = '2010-08-12 18:23:20,cobertura_vegetal,Bosques y Áreas Seminaturales,1\r\n'
    self.assertEquals(rec, output_data[0])

  def test_map_pipeline_prperty_map_filters(self):
    """ Property map filters filter out non matching records """

    # Prepare test data
    test_data = [
      {
        "record_type" : "test_record",
        "created_time" : datetime.datetime(2010,8,12,18,23,20),
        "data_quality" : 3,
        "schema_name" : "This is a, tests string",
        "expando_attr": "Some value here"
      },
      {
        "record_type" : "test_record",
        "created_time" : datetime.datetime(2010,9,11,19,20,21),
        "data_quality" : 4,
        "schema_name" : "One Value",
        "expando_attr": "Some other value here"
      },
      {
        "record_type" : "test_record",
        "created_time" : datetime.datetime(2011,9,11,19,20,21),
        "data_quality" : 1,
        "schema_name" : "some_schema_name",
        "expando_attr": "Some other value here"
      },
      {
        "record_type" : "test_record",
        "created_time" : datetime.datetime(2011,9,11,19,20,21),
        "data_quality" : 4,
        "schema_name" : "some_schema_name",
        "expando_attr": "One More Value"
      }
    ]

    for r in test_data:
      TestRecord(**r).put()

    output_data = _run_pipeline(
      self.taskqueue,
      __name__ + ".TestRecord",
      __name__ + ".record_map",
      params={
        "property_map": [{
          "property_match_name": "record_type",
          "property_match_value": "test_record",
          "property_filters": [
            ("data_quality", "=", 4)
          ],
          "property_list": [
            "created_time",
            "record_type",
            "expando_attr",
            "data_quality",
          ]
        }]
      }
    )

    # Asset Pipeline finished
    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith("Pipeline successful:"))

    # Assert number of rows, and format of one of them. Order is not predictable
    self.assertEquals(2, len(output_data))
    rec = output_data[0] if output_data[0].startswith('2010-') else output_data[1]
    self.assertEquals('2010-09-11 19:20:21,test_record,Some other value here,4\r\n', rec)

  def test_map_pipeline_does_not_yield_for_empty_properties(self):
    """ Property map filters filter out non matching records """

    # Prepare test data
    test_data = [
      {
        "record_type" : "test_record",
        "created_time" : None,
        "data_quality" : None,
        "schema_name" : ""
      }
    ]

    for r in test_data:
      TestRecord(**r).put()

    output_data = _run_pipeline(
      self.taskqueue,
      __name__ + ".TestRecord",
      __name__ + ".record_map",
      params={
        "property_map": [{
          "property_match_name": "record_type",
          "property_match_value": "test_record",
          "property_list": [
            "created_time",
            "expando_attr",
            "schema_name",
            "data_quality"
          ]
        }]
      }
    )

    # Asset Pipeline finished
    self.assertEquals(1, len(self.emails))
    self.assertTrue(self.emails[0][1].startswith("Pipeline successful:"))

    # Assert number of rows, and format of one of them. Order is not predictable
    self.assertEquals(0, len(output_data))
