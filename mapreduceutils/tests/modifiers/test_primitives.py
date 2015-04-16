import unittest
import datetime
from mapreduceutils.modifiers import primitives

class Record(object):
  pass


class TestDateFormatModifier(unittest.TestCase):

  def test_consistent_to_dict_module(self):
    """ DateFormatModifier consistently serializes  path """

    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%Y"}
    )
    self.assertEqual(
      'mapreduceutils.modifiers.primitives.DateFormatModifier',
      modifier.to_dict()['method']
    )

  def test_datetime_conversion(self):
    """ DateFormatModifier returns consistent values with strftime
        args on datetime objects """

    chain = {}
    record = Record()
    record.created_time = datetime.datetime(2010, 10, 3, 13, 20, 23, 5432)

    # Test to Year
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%Y"}
    )
    modifier.eval(record, chain)
    self.assertEqual('2010', chain['xxxx001'])

    # Test to Month
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%m"}
    )
    modifier.eval(record, chain)
    self.assertEqual('10', chain['xxxx001'])

    # Test to Day
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%d"}
    )
    modifier.eval(record, chain)
    self.assertEqual('03', chain['xxxx001'])

    # Test to Weekday
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%w"}
    )
    modifier.eval(record, chain)
    self.assertEqual('0', chain['xxxx001'])

    # Test to Year-Month-Day
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%Y-%m-%d"}
    )
    modifier.eval(record, chain)
    self.assertEqual('2010-10-03', chain['xxxx001'])

  def test_date_conversion(self):
    """ DateFormatModifier returns consistent values with
        strftime args on date objects """

    chain = {}
    record = Record()
    record.created_time = datetime.date(2010, 10, 3)

    # Test to Year
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%Y"}
    )
    modifier.eval(record, chain)
    self.assertEqual('2010', chain['xxxx001'])

    # Test to Month
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%m"}
    )
    modifier.eval(record, chain)
    self.assertEqual('10', chain['xxxx001'])

    # Test to Day
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%d"}
    )
    modifier.eval(record, chain)
    self.assertEqual('03', chain['xxxx001'])

    # Test to Weekday
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%w"}
    )
    modifier.eval(record, chain)
    self.assertEqual('0', chain['xxxx001'])

    # Test to Year-Month-Day
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%Y-%m-%d"}
    )
    modifier.eval(record, chain)
    self.assertEqual('2010-10-03', chain['xxxx001'])

  def test_date_return_guess(self):
    """ DateFormatModifier guesses consistent return values """

    record = Record()
    record.created_time = datetime.date(2010, 10, 3)

    # Test integers
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%Y"}
    )
    type_name = modifier.guess_return_type()
    self.assertEqual('int', type_name)

    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%m"}
    )

    type_name = modifier.guess_return_type()
    self.assertEqual('int', type_name)

    # Test to timestamp
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%s"}
    )
    type_name = modifier.guess_return_type()
    self.assertEqual('time', type_name)

    # Test to dash sep
    modifier = primitives.DateFormatModifier(
        identifier='xxxx001',
        operands={"value": "model.created_time"},
        arguments={"date_format": "%Y-%s"}
    )
    type_name = modifier.guess_return_type()
    self.assertEqual('basestring', type_name)



class TestCoerceToDateModifier(unittest.TestCase):
  def test_string_dates_coertion(self):
    """ CoerceToDateModifier converts distinct string formats
        to dates and datetimes """

    chain = {}
    record = Record()
    record.format_1 = "2010-02-09"
    record.format_2 = "09-02-2010"
    record.format_3 = "09/02/2010"
    record.format_4 = "2010-09-02 10:20"

    modifier = primitives.CoerceToDateModifier(
        identifier='xxxx001',
        operands={"value": "model.format_1"},
        arguments={"input_format": "%Y-%m-%d"}
    )

    test_date = datetime.datetime(2010, 2, 9, 0, 0)
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], datetime.date)
    self.assertEqual(test_date, chain['xxxx001'])

    modifier = primitives.CoerceToDateModifier(
        identifier='xxxx001',
        operands={"value": "model.format_2"},
        arguments={"input_format": "%d-%m-%Y"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], datetime.date)
    self.assertEqual(test_date, chain['xxxx001'])

    modifier = primitives.CoerceToDateModifier(
        identifier='xxxx001',
        operands={"value": "model.format_3"},
        arguments={"input_format": "%d/%m/%Y"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], datetime.date)
    self.assertEqual(test_date, chain['xxxx001'])

    modifier = primitives.CoerceToDateModifier(
        identifier='xxxx001',
        operands={"value": "model.format_4"},
        arguments={"input_format": "%Y-%m-%d %H:%M"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], datetime.date)
    self.assertEqual(datetime.datetime(2010, 9, 2, 10, 20), chain['xxxx001'])

  def test_bad_values_get_caught(self):
    """ CoerceToDateModifier generates an error on
        bad date values """

    chain = {}
    record = Record()
    record.format_1 = "2010-22-09"

    modifier = primitives.CoerceToDateModifier(
        identifier='xxxx001',
        operands={"value": "model.format_1"},
        arguments={"input_format": "%Y-%m-%d"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], basestring)
    self.assertEqual(
      "time data '2010-22-09' does not match format '%Y-%m-%d'",
      chain['xxxx001']
    )


class TestDateAddModifier(unittest.TestCase):
  def test_add_to_datetime(self):
    """ DateAddModifier can add seconds to datetime objects """

    chain = {}
    record = Record()
    record.date1 = datetime.datetime(2010, 1, 1, 0, 0)

    modifier = primitives.DateAddModifier(
        identifier='xxxx001',
        operands={"value": "model.date1"},
        arguments={"seconds": 86400}
    )
    modifier.eval(record, chain)
    self.assertEqual(datetime.datetime(2010, 1, 2, 0, 0), chain['xxxx001'])

    modifier = primitives.DateAddModifier(
        identifier='xxxx001',
        operands={"value": "model.date1"},
        arguments={"seconds": 86940}
    )
    modifier.eval(record, chain)
    self.assertEqual(datetime.datetime(2010, 1, 2, 0, 9), chain['xxxx001'])

  def test_add_to_date(self):
    """ DateAddModifier can add seconds to date objects"""

    chain = {}
    record = Record()
    record.date1 = datetime.date(2010, 1, 1)

    modifier = primitives.DateAddModifier(
        identifier='xxxx001',
        operands={"value": "model.date1"},
        arguments={"seconds": 86400}
    )
    modifier.eval(record, chain)
    self.assertEqual(datetime.date(2010, 1, 2), chain['xxxx001'])

    modifier = primitives.DateAddModifier(
        identifier='xxxx001',
        operands={"value": "model.date1"},
        arguments={"seconds": 86940}
    )
    modifier.eval(record, chain)
    self.assertEqual(datetime.date(2010, 1, 2), chain['xxxx001'])


class TestDateSubstractModifier(unittest.TestCase):
  def test_substract_dates(self):
    """ DateSubstractModifier can diff two date objects"""

    chain = {}
    record = Record()
    record.date1 = datetime.date(2010, 1, 1)
    record.date2 = datetime.date(2010, 2, 1)

    modifier = primitives.DateSubstractModifier(
        identifier='xxxx001',
        operands={"minuend": "model.date1", "subtrahend": "model.date2"},
        arguments={"diff_output": "days"}
    )
    modifier.eval(record, chain)
    self.assertEqual(-31, chain['xxxx001'])

  def test_substract_dates_in_years(self):
    """ DateSubstractModifier can diff two date objects in years """

    chain = {}
    record = Record()
    record.date1 = datetime.date(2010, 1, 1)
    record.date2 = datetime.date(2010, 2, 1)

    modifier = primitives.DateSubstractModifier(
        identifier='xxxx001',
        operands={"minuend": "model.date1", "subtrahend": "model.date2"},
        arguments={"diff_output": "years"}
    )
    modifier.eval(record, chain)
    self.assertEqual(-0.08493150684931507, chain['xxxx001'])

  def test_substract_dates_in_months(self):
    """ DateSubstractModifier can diff two date objects in months """

    chain = {}
    record = Record()
    record.date1 = datetime.date(2010, 1, 1)
    record.date2 = datetime.date(2010, 2, 1)

    modifier = primitives.DateSubstractModifier(
        identifier='xxxx001',
        operands={"minuend": "model.date1", "subtrahend": "model.date2"},
        arguments={"diff_output": "months"}
    )
    modifier.eval(record, chain)
    self.assertEqual(-1.0185037304068083, chain['xxxx001'])

  def test_substract_dates_in_days(self):
    """ DateSubstractModifier can diff two date objects in days """

    chain = {}
    record = Record()
    record.date1 = datetime.date(2010, 1, 1)
    record.date2 = datetime.date(2010, 2, 1)

    modifier = primitives.DateSubstractModifier(
        identifier='xxxx001',
        operands={"minuend": "model.date1", "subtrahend": "model.date2"},
        arguments={"diff_output": "days"}
    )
    modifier.eval(record, chain)
    self.assertEqual(-31, chain['xxxx001'])

  def test_substract_dates_in_hours(self):
    """ DateSubstractModifier can diff two date objects in hours """

    chain = {}
    record = Record()
    record.date1 = datetime.date(2010, 1, 1)
    record.date2 = datetime.date(2010, 2, 1)

    modifier = primitives.DateSubstractModifier(
        identifier='xxxx001',
        operands={"minuend": "model.date1", "subtrahend": "model.date2"},
        arguments={"diff_output": "hours"}
    )
    modifier.eval(record, chain)
    self.assertEqual(-744, chain['xxxx001'])

  def test_substract_dates_in_seconds(self):
    """ DateSubstractModifier can diff two date objects in seconds """

    chain = {}
    record = Record()
    record.date1 = datetime.date(2010, 1, 1)
    record.date2 = datetime.date(2010, 2, 1)

    modifier = primitives.DateSubstractModifier(
        identifier='xxxx001',
        operands={"minuend": "model.date1", "subtrahend": "model.date2"},
        arguments={"diff_output": "seconds"}
    )
    modifier.eval(record, chain)
    self.assertEqual(-2678400, chain['xxxx001'])

  def test_substract_datetimes_in_seconds(self):
    """ DateSubstractModifier can diff two datetime objects in seconds """

    chain = {}
    record = Record()
    record.date1 = datetime.datetime(2010, 1, 1, 0, 0)
    record.date2 = datetime.datetime(2010, 2, 1, 3, 20)

    modifier = primitives.DateSubstractModifier(
        identifier='xxxx001',
        operands={"minuend": "model.date1", "subtrahend": "model.date2"},
        arguments={"diff_output": "seconds"}
    )
    modifier.eval(record, chain)
    self.assertEqual(-2690400, chain['xxxx001'])

  def test_absolute_modifier(self):
    """ DateSubstractModifier can return date diffs without sign """

    chain = {}
    record = Record()
    record.date1 = datetime.date(2010, 1, 1)
    record.date2 = datetime.date(2010, 2, 1)

    modifier = primitives.DateSubstractModifier(
        identifier='xxxx001',
        operands={"minuend": "model.date1", "subtrahend": "model.date2"},
        arguments={"diff_output": "days", "absolute_value": True}
    )
    modifier.eval(record, chain)
    self.assertEqual(31, chain['xxxx001'])


class TestCoerceToNumberModifier(unittest.TestCase):
  def test_int_or_float_from_str(self):
    """ CoerceToNumberModifier can return int and float from str """

    chain = {}
    record = Record()
    record.strval = '1'
    record.strval2 = '1.0'

    modifier = primitives.CoerceToNumberModifier(
        identifier='xxxx001',
        operands={"value": "model.strval"},
        arguments={"type": "int"}
    )
    modifier.eval(record, chain)
    self.assertEqual(1, chain['xxxx001'])

    modifier = primitives.CoerceToNumberModifier(
        identifier='xxxx001',
        operands={"value": "model.strval"},
        arguments={"type": "float"}
    )
    modifier.eval(record, chain)
    self.assertEqual(1.0, chain['xxxx001'])

    modifier = primitives.CoerceToNumberModifier(
        identifier='xxxx001',
        operands={"value": "model.strval2"},
        arguments={"type": "float"}
    )
    modifier.eval(record, chain)
    self.assertEqual(1.0, chain['xxxx001'])

  def test_int_from_int_or_float(self):
    """ CoerceToNumberModifier can return int from int and float"""

    chain = {}
    record = Record()
    record.intval = 1
    record.floatval = 1.0

    modifier = primitives.CoerceToNumberModifier(
        identifier='xxxx001',
        operands={"value": "model.intval"},
        arguments={"type": "int"}
    )
    modifier.eval(record, chain)
    self.assertEqual(1, chain['xxxx001'])

    modifier = primitives.CoerceToNumberModifier(
        identifier='xxxx001',
        operands={"value": "model.floatval"},
        arguments={"type": "int"}
    )
    modifier.eval(record, chain)
    self.assertEqual(1, chain['xxxx001'])

  def test_float_from_int_or_float(self):
    """ CoerceToNumberModifier can return int from int and float"""

    chain = {}
    record = Record()
    record.intval = 1
    record.floatval = 1.0

    modifier = primitives.CoerceToNumberModifier(
        identifier='xxxx001',
        operands={"value": "model.intval"},
        arguments={"type": "float"}
    )
    modifier.eval(record, chain)
    self.assertEqual(1.0, chain['xxxx001'])

    modifier = primitives.CoerceToNumberModifier(
        identifier='xxxx001',
        operands={"value": "model.floatval"},
        arguments={"type": "float"}
    )
    modifier.eval(record, chain)
    self.assertEqual(1.0, chain['xxxx001'])


class TestConstantValueModifier(unittest.TestCase):
  def test_constant_int(self):
    """ ConstantValueModifier returns int values for int type constants """

    chain = {}
    record = Record()
    modifier = primitives.ConstantValueModifier(
        identifier='xxxx001',
        arguments={"value": "1", "type": "int"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], int)
    self.assertEqual(1, chain['xxxx001'])

  def test_constant_str(self):
    """ ConstantValueModifier returns str values for str type constants """

    chain = {}
    record = Record()
    modifier = primitives.ConstantValueModifier(
        identifier='xxxx001',
        arguments={"value": "1", "type": "str"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], basestring)
    self.assertEqual('1', chain['xxxx001'])

  def test_constant_float(self):
    """ ConstantValueModifier returns float values for float type constants """

    chain = {}
    record = Record()
    modifier = primitives.ConstantValueModifier(
        identifier='xxxx001',
        arguments={"value": "1", "type": "float"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], float)
    self.assertEqual(1.0, chain['xxxx001'])

  def test_non_coercible(self):
    """ ConstantValueModifier returns errors when values are not coercible"""

    chain = {}
    record = Record()
    modifier = primitives.ConstantValueModifier(
        identifier='xxxx001',
        arguments={"value": "x", "type": "float"}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], basestring)
    self.assertEquals('could not convert string to float: x', chain['xxxx001'])


class TestRoundNumberModifier(unittest.TestCase):
  def test_round_int(self):
    """ RoundNumberModifier returns int values for 0 rounding """

    chain = {}
    record = Record()
    modifier = primitives.RoundNumberModifier(
        identifier='xxxx001',
        operands={"value": 1.34},
        arguments={"ndigits": 0}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], int)
    self.assertEqual(1, chain['xxxx001'])

  def test_round_float(self):
    """ RoundNumberModifier returns int values for 0 rounding """

    chain = {}
    record = Record()
    modifier = primitives.RoundNumberModifier(
        identifier='xxxx001',
        operands={"value": 1.34333},
        arguments={"ndigits": 1}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], float)
    self.assertEqual(1.3, chain['xxxx001'])


class TestFloorNumberModifier(unittest.TestCase):
  def test_floor_value(self):
    """ FloorNumberModifier modifies values consistently """

    chain = {}
    record = Record()
    modifier = primitives.FloorNumberModifier(
        identifier='xxxx001',
        operands={"value": 1.34}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], float)
    self.assertEqual(1.0, chain['xxxx001'])

  def test_round_float(self):
    """ RoundNumberModifier returns int values for 0 rounding """

    chain = {}
    record = Record()
    modifier = primitives.RoundNumberModifier(
        identifier='xxxx001',
        operands={"value": 1.34333},
        arguments={"ndigits": 1}
    )
    modifier.eval(record, chain)
    self.assertIsInstance(chain['xxxx001'], float)
    self.assertEqual(1.3, chain['xxxx001'])


class TestBooleanLogicModifier(unittest.TestCase):
  def test_eval_and_true(self):
    """ LogicANDModifier Evaluates AND expressions to True """

    chain = {'id0001': True, 'id0002': True}
    record = Record()
    modifier = primitives.BooleanLogicModifier(
        identifier='xxxx001',
        operands={"x": 'identifier.id0001', "y": 'identifier.id0002'},
        arguments={"operation": 'AND', "true_value": 'ABC', "false_value": 'BCD'}
    )
    modifier.eval(record, chain)
    self.assertEqual('ABC', chain['xxxx001'])

  def test_eval_and_false(self):
    """ LogicANDModifier Evaluates AND expressions to False """

    chain = {'id0001': True, 'id0002': False}
    record = Record()
    modifier = primitives.BooleanLogicModifier(
        identifier='xxxx001',
        operands={"x": 'identifier.id0001', "y": 'identifier.id0002'},
        arguments={"operation": 'AND', "true_value": 'ABC', "false_value": 'BCD'}
    )
    modifier.eval(record, chain)
    self.assertEqual('BCD', chain['xxxx001'])

class TestTextMatchModifier(unittest.TestCase):
    def test_startswith_match(self):
      """ TextMatchModifier startswith matches OK """

      chain = {}
      record = Record()
      record.text = "The Mongoose came a long way along the river"
      modifier = primitives.TextMatchModifier(
        identifier="xx0001",
        operands={"value": "model.text"},
        arguments={"operation": "starts",
                   "needle": "The",
                   "true_value": "Yeah!",
                   "false_value": "Nope!"}
      )
      modifier.eval(record, chain)
      self.assertEqual('Yeah!', chain['xx0001'])

    def test_startswith_nonmatch(self):
      """ TextMatchModifier performs startswith non matches OK """

      chain = {}
      record = Record()
      record.text = "The Mongoose came a long way along the river"
      modifier = primitives.TextMatchModifier(
        identifier="xx0001",
        operands={"value": "model.text"},
        arguments={"operation": "starts",
                   "needle": "Mongoose",
                   "true_value": "Yeah!",
                   "false_value": "Nope!"}
      )
      modifier.eval(record, chain)
      self.assertEqual('Nope!', chain['xx0001'])

    def test_endswith_match(self):
      """ TextMatchModifier endswith matching OK """

      chain = {}
      record = Record()
      record.text = "The Mongoose came a long way along the river"
      modifier = primitives.TextMatchModifier(
        identifier="xx0001",
        operands={"value": "model.text"},
        arguments={"operation": "ends",
                   "needle": "river",
                   "true_value": "Yeah!",
                   "false_value": "Nope!"}
      )
      modifier.eval(record, chain)
      self.assertEqual('Yeah!', chain['xx0001'])

    def test_endswith_nonmatch(self):
      """ TextMatchModifier endswith non matching OK """

      chain = {}
      record = Record()
      record.text = "The Mongoose came a long way along the river"
      modifier = primitives.TextMatchModifier(
        identifier="xx0001",
        operands={"value": "model.text"},
        arguments={"operation": "ends",
                   "needle": "along",
                   "true_value": "Yeah!",
                   "false_value": "Nope!"}
      )
      modifier.eval(record, chain)
      self.assertEqual('Nope!', chain['xx0001'])

    def test_contains_match(self):
      """ TextMatchModifier contains matching OK """

      chain = {}
      record = Record()
      record.text = "The Mongoose came a long way along the river"
      modifier = primitives.TextMatchModifier(
        identifier="xx0001",
        operands={"value": "model.text"},
        arguments={"operation": "contains",
                   "needle": "way",
                   "true_value": "Yeah!",
                   "false_value": "Nope!"}
      )
      modifier.eval(record, chain)
      self.assertEqual('Yeah!', chain['xx0001'])

    def test_contains_nonmatch(self):
      """ TextMatchModifier contains non matching OK """

      chain = {}
      record = Record()
      record.text = "The Mongoose came a long way along the river"
      modifier = primitives.TextMatchModifier(
        identifier="xx0001",
        operands={"value": "model.text"},
        arguments={"operation": "contains",
                   "needle": "highway",
                   "true_value": "Yeah!",
                   "false_value": "Nope!"}
      )
      modifier.eval(record, chain)
      self.assertEqual('Nope!', chain['xx0001'])


class TestArithmeticModifier(unittest.TestCase):

  def test_add_operation(self):
    """ Arithmetic modifier add operation OK """

    chain = {}
    record = Record()
    record.value_a = 3
    record.value_b = 4
    record.value_c = 1.0

    """ Add two integers """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0001",
      operands={"x": "model.value_a", "y": "model.value_b"},
      arguments={"expression": "x+y"}
    )
    modifier.eval(record, chain)
    self.assertEqual(7, chain['xxx0001'])

    """ Add a float and an integer """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0002",
      operands={"x": "model.value_a", "y": "model.value_c"},
      arguments={"expression": "x+y"}
    )
    modifier.eval(record, chain)
    self.assertEqual(4.0, chain['xxx0002'])

  def test_substract_operation(self):
    """ Arithmetic modifier substraction operation OK """

    chain = {}
    record = Record()
    record.value_a = 3
    record.value_b = 4
    record.value_c = 1.0

    """ Substract two integers """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0001",
      operands={"x": "model.value_b", "y": "model.value_a"},
      arguments={"expression": "x-y"}
    )
    modifier.eval(record, chain)
    self.assertEqual(1, chain['xxx0001'])

    """ Substract a float and an integer """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0002",
      operands={"x": "model.value_b", "y": "model.value_c"},
      arguments={"expression": "x-y"}
    )
    modifier.eval(record, chain)
    self.assertEqual(3.0, chain['xxx0002'])

  def test_multiply_operation(self):
    """ Arithmetic modifier multiply operation OK """

    chain = {}
    record = Record()
    record.value_a = 3
    record.value_b = 4
    record.value_c = 1.0

    """ Multiply two integers """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0001",
      operands={"x": "model.value_b", "y": "model.value_a"},
      arguments={"expression": "x*y"}
    )
    modifier.eval(record, chain)
    self.assertEqual(12, chain['xxx0001'])

    """ Multiply a float and an integer """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0002",
      operands={"x": "model.value_b", "y": "model.value_c"},
      arguments={"expression": "x*y"}
    )
    modifier.eval(record, chain)
    self.assertEqual(4.0, chain['xxx0002'])

  def test_divide_operation(self):
    """ Arithmetic modifier divide operation OK """

    chain = {}
    record = Record()
    record.value_a = 3
    record.value_b = 4
    record.value_c = 1.0
    record.value_d = 4.0
    record.value_e = 3.0

    """ Divide two integers """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0001",
      operands={"x": "model.value_b", "y": "model.value_a"},
      arguments={"expression": "x/y"}
    )
    modifier.eval(record, chain)
    self.assertEqual(1, chain['xxx0001'])

    """ Divide a float and an integer """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0002",
      operands={"x": "model.value_b", "y": "model.value_c"},
      arguments={"expression": "x/y"}
    )
    modifier.eval(record, chain)
    self.assertEqual(4.0, chain['xxx0002'])

    """ Divide two floats """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0003",
      operands={"x": "model.value_d", "y": "model.value_e"},
      arguments={"expression": "x/y"}
    )
    modifier.eval(record, chain)
    self.assertAlmostEqual(1.33333333, chain['xxx0003'])

  def test_modulo_operation(self):
    """ Arithmetic modifier modulo operation OK """

    chain = {}
    record = Record()
    record.value_a = 3
    record.value_b = 4
    record.value_c = 1.0
    record.value_d = 4.0
    record.value_e = 3.0

    """ Modulo of two integers """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0001",
      operands={"x": "model.value_b", "y": "model.value_a"},
      arguments={"expression": "x%y"}
    )
    modifier.eval(record, chain)
    self.assertEqual(1, chain['xxx0001'])

    """ Modulo of a float and an integer """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0002",
      operands={"x": "model.value_b", "y": "model.value_c"},
      arguments={"expression": "x%y"}
    )
    modifier.eval(record, chain)
    self.assertEqual(0.0, chain['xxx0002'])

    """ Modulo of two floats """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0003",
      operands={"x": "model.value_d", "y": "model.value_e"},
      arguments={"expression": "x%y"}
    )
    modifier.eval(record, chain)
    self.assertAlmostEqual(1.0, chain['xxx0003'])

  def test_nested_formula(self):
    """ Arithmetic modifier nested operations OK """

    chain = {}
    record = Record()
    record.value_a = 3
    record.value_b = 4
    record.value_c = 1.0
    record.value_d = 4.0
    record.value_e = 3.0

    """ Modulo of two integers """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0001",
      operands={"x": "model.value_b", "y": "model.value_a", "z": 3.4},
      arguments={"expression": "(x*y) + (2*z)"}
    )
    modifier.eval(record, chain)
    self.assertEqual(18.8, chain['xxx0001'])

  def test_nested_formula_with_names(self):
    """ Arithmetic modifier nested operations with variable names OK """

    chain = {}
    record = Record()
    record.value_a = 3
    record.value_b = 4
    record.value_c = 1.0
    record.value_d = 4.0
    record.value_e = 3.0

    """ Modulo of two integers """
    modifier = primitives.ArithmeticModifier(
      identifier="xxx0001",
      operands={
        "some_val": "model.value_b",
        "other_val": "model.value_a",
        "z_val": 3.4
      },
      arguments={"expression": "(some_val * other_val) + (2 * z_val)"}
    )
    modifier.eval(record, chain)
    self.assertEqual(18.8, chain['xxx0001'])

if __name__ == '__main__':
  suite = unittest.TestSuite()
  suite.addTest(unittest.makeSuite(TestDateFormatModifier))
  suite.addTest(unittest.makeSuite(TestCoerceToDateModifier))
  suite.addTest(unittest.makeSuite(TestDateAddModifier))
  suite.addTest(unittest.makeSuite(TestDateSubstractModifier))
  suite.addTest(unittest.makeSuite(TestConstantValueModifier))
  suite.addTest(unittest.makeSuite(TestCoerceToNumberModifier))
  suite.addTest(unittest.makeSuite(TestBooleanLogicModifier))
  suite.addTest(unittest.makeSuite(TestTextMatchModifier))
  suite.addTest(unittest.makeSuite(TestArithmeticModifier))
  unittest.TextTestRunner(verbosity=2).run(suite)
