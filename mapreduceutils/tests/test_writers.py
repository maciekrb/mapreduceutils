#!/usr/bin/env python
from collections import OrderedDict
import datetime
from mapreduceutils.writers import OutputWriter
import unittest


class TestCSVOutputWriter(unittest.TestCase):

  def test_simple_output(self):
    record = OrderedDict([
      ("sprop_abc", 1),
      ("sprop_bcd", "test message"),
      ("sprop_cde", None)
    ])
    writer = OutputWriter.get_writer('csv')
    out = writer.write(record)

    self.assertEqual("1,test message,\r\n", out)

  def test_escaping(self):
    record = OrderedDict([
      ("sprop_abc", 1),
      ("sprop_bcd", "test,message"),
      ("sprop_cde", None)
    ])
    writer = OutputWriter.get_writer('csv')
    out = writer.write(record)

    self.assertEqual('1,"test,message",\r\n', out)


class TestJSONOutputWriter(unittest.TestCase):

  def test_simple_output(self):
    record = OrderedDict([
      ("sprop_abc", 1),
      ("sprop_bcd", "test message"),
      ("sprop_cde", None)
    ])
    writer = OutputWriter.get_writer('json')
    out = writer.write(record)

    expected = (
      '{'
      '"sprop_abc": 1, '
      '"sprop_bcd": "test message", '
      '"sprop_cde": null'
      '}\r\n'
    )
    self.assertEqual(expected, out)

  def test_escaped_output(self):
    record = OrderedDict([
      ("sprop_abc", 1),
      ("sprop_bcd", 'test "message"'),
      ("sprop_cde", None)
    ])
    writer = OutputWriter.get_writer('json')
    out = writer.write(record)

    expected = (
      '{'
      '"sprop_abc": 1, '
      '"sprop_bcd": "test \\"message\\"", '
      '"sprop_cde": null'
      '}\r\n'
    )
    self.assertEqual(expected, out)

  def test_nan_as_null_output(self):
    record = OrderedDict([
      ("sprop_abc", float('NaN')),
      ("sprop_bcd", 'test "message"'),
      ("sprop_cde", datetime.datetime(2014, 8, 1, 0, 0, 0))
    ])
    writer = OutputWriter.get_writer('json')
    out = writer.write(record, nan_to_null=True)

    expected = (
      '{'
      '"sprop_cde": "2014-08-01 00:00:00", '
      '"sprop_bcd": "test \\"message\\"", '
      '"sprop_abc": null'
      '}\r\n'
    )
    self.assertEqual(expected, out)
