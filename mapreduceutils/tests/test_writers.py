#!/usr/bin/env python
from collections import OrderedDict
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
