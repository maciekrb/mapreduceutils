import unittest
import datetime
from datastoreutils.modifiers import primitives

class Record(object):
  pass

class TestDateModifier(unittest.TestCase):

  def test_datetime_conversion(self):
    """ DateModifier returns consistent values with strftime args on datetime objects """

    chain = {}
    record = Record()
    record.created_time = datetime.datetime(2010, 10, 3, 13, 20, 23, 5432)

    # Test to Year
    modifier = primitives.DateModifier(
        identifier='xxxx001',
        operands={"date": "model.created_time"},
        arguments={"date_format": "%Y"}
    )
    modifier.eval(record, chain)
    self.assertEqual('2010', chain['xxxx001'])

    # Test to Month
    modifier = primitives.DateModifier(
        identifier='xxxx001',
        operands={"date": "model.created_time"},
        arguments={"date_format": "%m"}
    )
    modifier.eval(record, chain)
    self.assertEqual('10', chain['xxxx001'])

    # Test to Day
    modifier = primitives.DateModifier(
        identifier='xxxx001',
        operands={"date": "model.created_time"},
        arguments={"date_format": "%d"}
    )
    modifier.eval(record, chain)
    self.assertEqual('03', chain['xxxx001'])

    # Test to Weekday
    modifier = primitives.DateModifier(
        identifier='xxxx001',
        operands={"date": "model.created_time"},
        arguments={"date_format": "%w"}
    )
    modifier.eval(record, chain)
    self.assertEqual('0', chain['xxxx001'])

    # Test to Year-Month-Day
    modifier = primitives.DateModifier(
        identifier='xxxx001',
        operands={"date": "model.created_time"},
        arguments={"date_format": "%Y-%m-%d"}
    )
    modifier.eval(record, chain)
    self.assertEqual('2010-10-03', chain['xxxx001'])

  def test_date_conversion(self):
    """ DateModifier returns consistent values with strftime args on date objects """

    chain = {}
    record = Record()
    record.created_time = datetime.date(2010, 10, 3)

    # Test to Year
    modifier = primitives.DateModifier(
        identifier='xxxx001',
        operands={"date": "model.created_time"},
        arguments={"date_format": "%Y"}
    )
    modifier.eval(record, chain)
    self.assertEqual('2010', chain['xxxx001'])

    # Test to Month
    modifier = primitives.DateModifier(
        identifier='xxxx001',
        operands={"date": "model.created_time"},
        arguments={"date_format": "%m"}
    )
    modifier.eval(record, chain)
    self.assertEqual('10', chain['xxxx001'])

    # Test to Day
    modifier = primitives.DateModifier(
        identifier='xxxx001',
        operands={"date": "model.created_time"},
        arguments={"date_format": "%d"}
    )
    modifier.eval(record, chain)
    self.assertEqual('03', chain['xxxx001'])

    # Test to Weekday
    modifier = primitives.DateModifier(
        identifier='xxxx001',
        operands={"date": "model.created_time"},
        arguments={"date_format": "%w"}
    )
    modifier.eval(record, chain)
    self.assertEqual('0', chain['xxxx001'])

    # Test to Year-Month-Day
    modifier = primitives.DateModifier(
        identifier='xxxx001',
        operands={"date": "model.created_time"},
        arguments={"date_format": "%Y-%m-%d"}
    )
    modifier.eval(record, chain)
    self.assertEqual('2010-10-03', chain['xxxx001'])


class TestConstantModifier(unittest.TestCase):
  def test_constant_int(self):
    """ ConstantModifier returns int values for int type constants """

    chain = {}
    record = Record()
    modifier = primitives.ConstantModifier(
        identifier='xxxx001',
        arguments={"value": "1", "type": "int"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], int)
    self.assertEqual(1, chain['xxxx001'])

  def test_constant_str(self):
    """ ConstantModifier returns str values for str type constants """

    chain = {}
    record = Record()
    modifier = primitives.ConstantModifier(
        identifier='xxxx001',
        arguments={"value": "1", "type": "str"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], basestring)
    self.assertEqual('1', chain['xxxx001'])

  def test_constant_float(self):
    """ ConstantModifier returns float values for float type constants """

    chain = {}
    record = Record()
    modifier = primitives.ConstantModifier(
        identifier='xxxx001',
        arguments={"value": "1", "type": "float"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], float)
    self.assertEqual(1.0, chain['xxxx001'])

  def test_non_coercible(self):
    """ ConstantModifier returns errors when values are not coercible"""

    chain = {}
    record = Record()
    modifier = primitives.ConstantModifier(
        identifier='xxxx001',
        arguments={"value": "x", "type": "float"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], basestring)
    self.assertEquals('could not convert string to float: x', chain['xxxx001'])

if __name__ == '__main__':
  suite = unittest.TestSuite()
  suite.addTest(unittest.makeSuite(TestDateModifier))
  unittest.TextTestRunner(verbosity=2).run(suite)
