"""
Basice value modifier classes

Implements Basic format and logic modifiers
"""
import logging
import calendar
import datetime
from babel.dates import (
  format_date,
  format_datetime
)
from pytz import timezone
import re
from . import FieldModifier
from math import ceil, floor
from py_expression_eval import Parser
from ..utils import posix2LDML
from google.appengine.ext import ndb
from google.appengine.datastore import datastore_query
from google.net.proto.ProtocolBuffer import ProtocolBufferDecodeError


class DateFormatModifier(FieldModifier):
  """ Performs strftime on date/datetime objects """

  META_NAME = 'Date Formatter'
  META_DESCRIPTION = 'Changes the format of a date / datetime object'
  META_OPERANDS = {
    "value": {
      "name": "Date object",
      "description": "The provided object value will be converted to the given date format",
      "valid_types": [datetime.date, datetime.datetime]
    }
  }
  META_ARGS = {
    "date_format": {
      "name": "Output format",
      "description": "Desired Date Time field output format",
      "type": basestring,
      "options": {
        "%Y-%m-%d": 'Year, month and day as decimal numbers',
        "%Y-%m": 'Year and month as decimal numbers',
        "%Y-%W": 'Year and week number as decimal numbers',
        "%a": 'Weekday as locale abbreviated name (Sun, Mon)',
        "%A": 'Weekday as locale full name (Sunday, Monday)',
        "%w": 'Weekday as decimal number, where 0 is Sunday and 6 is Saturday',
        "%d": 'Day of month as zero-padded decimal number',
        "%b": 'Month as locale abbreviated name',
        "%B": 'Month as locale full name',
        "%m": 'Month as zero-padded decimal number',
        "%Y": 'Year with century as decimal number',
        "%j": 'Day of the year as a zero-padded decimal number',
        "%W": 'Week number of the year (Monday as first day of week) as decimal number'
      }
    },
    "locale": {
      "name": "Locale",
      "description": "Locale for month, day names, ie 'es_CO'",
      "type": basestring
    },
    "timezone": {
      "name": "Time Zone",
      "description": "time zone if output should be different from UTC. ie. 'Europe/London'",
      "type": basestring
    }
  }

  def _from_strftime(self, date_format):
      fn = getattr(self.get_operand('value'), 'strftime')
      return fn(date_format)

  def _evaluate(self):
    date_format = self.get_argument('date_format')
    if date_format in ('%s', '%w'):  # not in LDML
      return self._from_strftime(date_format)

    ldml_format = posix2LDML(date_format)  # compat with babel

    locale = self.get_argument('locale', 'en_US')
    value = self.get_operand('value')
    if isinstance(value, datetime.datetime):
      tzinfo = timezone(self.get_argument('timezone', 'UTC'))
      return format_datetime(value, format=ldml_format, tzinfo=tzinfo, locale=locale)

    elif isinstance(value, datetime.date):
      return format_date(value, format=ldml_format, locale=locale)

  def guess_return_type(self):
    date_format = self.get_argument('date_format')
    if date_format == '%s':
      return datetime.time.__name__
    elif re.search('%a|%A|%b|%B|%j|-|/', date_format):
      return basestring.__name__
    elif date_format in ('%Y', '%m', '%W', '%w', '%j'):
      return int.__name__


class DaysInMonthModifier(FieldModifier):
  """ Obtains the number of days for the month in given date/datetime """

  META_NAME = 'Days in month'
  META_DESCRIPTION = 'Obtains the number of days in a month'
  META_OPERANDS = {
    "value": {
      "name": "Date object",
      "description": "Number of days in month will be obtained from the object",
      "valid_types": [datetime.date, datetime.datetime]
    }
  }
  META_ARGS = {}

  def _evaluate(self):
    obj = self.get_operand('value')
    year = int(obj.strftime("%Y"))
    month = int(obj.strftime("%m"))
    cal_out = calendar.monthrange(year, month)
    return cal_out[1]


class CoerceToDateModifier(FieldModifier):
  """ Coerces a string into a datetime object """

  META_NAME = 'Convert a string to a date'
  META_DESCRIPTION = 'Coerces a string into a datetime object'
  META_OPERANDS = {
    "value": {
      "name": "Object to convert to date",
      "description": "Object to convert",
      "valid_types": [basestring, float, int]
    }
  }
  META_ARGS = {
    "input_format": {
      "name": "Date input format",
      "description": "Format in which the input should be parsed",
      "type": basestring,
      "options": {
        "%Y-%m-%d": "YYYY-MM-DD",
        "%Y-%m-%d %H:%M": "YYYY-MM-DD hh:mm",
        "%d-%m-%Y": "DD-MM-YYYY",
        "%d-%d-%Y %H:%M": "DD-MM-YYYY hh:mm",
        "%Y/%m/%d": "YYYY/MM/DD",
        "%d/%m/%Y": "DD/MM/YYYY",
      }
    }
  }

  def _evaluate(self):
    try:
      value = datetime.datetime.strptime(
        self.get_operand('value'),
        self.get_argument('input_format')
      )
    except ValueError as e:
      value = e.message
    finally:
      return value

  def guess_return_type(self):
    return datetime.__name__


class DateAddModifier(FieldModifier):
  """ Adds two dates """

  META_NAME = 'Add seconds to date or datetime'
  META_DESCRIPTION = None
  META_OPERANDS = {
    "value": {
      "name": "date/datetime object",
      "description": "Date object to which seconds will be added",
      "valid_types": [datetime.date, datetime.datetime]
    }
  }
  META_ARGS = {
    "seconds": {
      "name": "Seconds",
      "description": "Number of seconds to add to value",
      "type": int
    }
  }

  def _evaluate(self):
    value = self.get_operand('value')

    if not isinstance(value, (datetime.date, datetime.datetime)):
      return "value is not a date/datetime object"

    return value + datetime.timedelta(seconds=self.get_argument('seconds'))

  def guess_return_type(self):
    return datetime.__name__


class DateSubstractModifier(FieldModifier):
  META_NAME = 'Date substraction'
  META_DESCRIPTION = "Substracts one date from another"
  META_OPERANDS = {
    "minuend": {
      "name": "minuend date/datetime",
      "description": "Date from which annother date will be substracted",
      "valid_types": [datetime.date, datetime.datetime]
    },
    "subtrahend": {
      "name": "subtrahend date/datetime",
      "description": "Date which will be substracted from minuend",
      "valid_types": [datetime.date, datetime.datetime]
    }
  }
  META_ARGS = {
    "diff_output": {
      "name": "Output in",
      "description": "Desired type of output",
      "type": basestring,
      "options": {
        "years": "Years",
        "months": "Months",
        "days": "Days",
        "hours": "Hours",
        "minutes": "Minutes",
        "seconds": "Seconds"
      }
    },
    "absolute_value": {
      "name": "Absolute value",
      "description": "If set to true, will not add the - signed if difference is negative",
      "type": bool
    }
  }

  def _evaluate(self):
    minuend = self.get_operand('minuend')
    subtrahend = self.get_operand('subtrahend')
    output = self.get_argument('diff_output')
    absolute = self.get_argument('absolute_value')

    res = minuend - subtrahend
    secs = res.days * 86400 + res.seconds

    if output == 'years':
      out = float(secs) / 31536000.0
    elif output == 'months':
      out = float(secs) / 2629740.0
    elif output == 'days':
      out = float(secs) / 86400.0
    elif output == 'hours':
      out = float(secs) / 3600
    elif output == 'seconds':
      out = secs

    return abs(out) if absolute is True else out

  def guess_return_type(self):
    return datetime.__name__


class ConstantValueModifier(FieldModifier):
  """ Allows to generate a constant value """

  META_NAME = "Constant Value"
  META_DESCRIPTION = "Allows to generate constant values for a column"
  META_OPERANDS = {}  # No operands are required for constant value generation
  META_ARGS = {
    "value": {
      "name": "Constant value",
      "description": "The constant value that should be generated",
      "type": basestring
    },
    "type": {
      "name": "Data type",
      "description": "Data type of the generated value",
      "type": basestring,
      "options": {
        "int": "Integer value",
        "str": "String Value",
        "float": "Floating point value"
      }
    }
  }

  def _evaluate(self):
    vtype = self.get_argument('type')
    value = self.get_argument('value')
    try:
      if vtype == 'str':
        return unicode(value)
      elif vtype == 'int':
        return int(value)
      elif vtype == 'float':
        return float(value)
      else:
        raise NotImplemented("Type {} has not been implemented in ConstantModifier".format(vtype))
    except ValueError as e:
      return e.message

  def guess_return_type(self):
    vtype = self.get_argument('type')
    if vtype == 'str':
      return basestring.__name__
    elif vtype == 'int':
      return int.__name__
    elif vtype == 'float':
      return float.__name__


class CoerceToNumberModifier(FieldModifier):
  """ Coerces a string/float into an int, so operations can be performed with values """

  META_NAME = 'Convert a string to an Integer'
  META_DESCRIPTION = 'Coerces a string or float to an integer/float value, so it can be used to perform  operations'
  META_OPERANDS = {
    "value": {
      "name": "Object to convert",
      "description": "Object to convert",
      "valid_types": [basestring, float, int]
    }
  }
  META_ARGS = {
    "type": {
      "name": "Desired type",
      "description": "Type to which coercion will be attempted",
      "type": basestring,
      "options": {
        "int": "Integer value",
        "float": "Floating point value"
      }
    }
  }

  def _evaluate(self):

    out_type = self.get_argument('type')
    number = self.get_operand('value')

    try:
      if isinstance(number, float):
        out = int(number) if out_type == "int" else number

      if isinstance(number, int):
        out = float(number) if out_type == "float" else number

      if isinstance(number, basestring):
        if out_type == "int":
          out = int(number)
        elif out_type == "float":
          out = float(number)

    except ValueError as e:
      out = e.message

    finally:
      return out

  def guess_return_type(self):
    vtype = self.get_argument('type')
    if vtype == 'int':
      return int.__name__
    elif vtype == 'float':
      return float.__name__


class RoundNumberModifier(FieldModifier):
  """ Rounds a decimal value """

  META_NAME = 'Round number'
  META_DESCRIPTION = 'Rounds a decimal value'
  META_OPERANDS = {
    "value": {
      "name": "Float column",
      "description": None,
      "valid_types": [float]
    }
  }
  META_ARGS = {
    "ndigits": {
      "name": "Number of decimal places",
      "description": "Number of desired decimal places for output value",
      "type": int
    }
  }

  def _evaluate(self):
    ndigits = self.get_argument('ndigits')
    number = self.get_operand('value')
    if ndigits == 0:
      return int(number)
    return round(number, ndigits)

  def guess_return_type(self):
    ndigits = self.get_argument('ndigits')
    if ndigits == 0:
      return int.__name__
    else:
      return float.__name__


class FloorNumberModifier(FieldModifier):
  """ Performs floor() on value """

  META_NAME = 'Floor number'
  META_DESCRIPTION = 'Get the closest integer value less or equal than value as a float'
  META_OPERANDS = {
    "value": {
      "name": "Decimal number",
      "description": "Decimal number to transform",
      "valid_types": [float]
    }
  }
  META_ARGS = {}

  def _evaluate(self):
    number = self.get_operand('value')
    return floor(number)

  def guess_return_type(self):
    return float.__name__


class CeilNumberModifier(FieldModifier):
  """ Performs ceil() on value """

  META_NAME = 'Ceil number'
  META_DESCRIPTION = 'Get the closest integer value greater or equal than value as a float'
  META_OPERANDS = {
    "value": {
      "name": "Decimal number",
      "description": "Decimal number to transform",
      "valid_types": [float]
    }
  }
  META_ARGS = {}

  def _evaluate(self):
    number = self.get_operand('value')
    return ceil(number)

  def guess_return_type(self):
    return float.__name__


class BooleanLogicModifier(FieldModifier):
  """ Performs Boolean logic evaluation on terms """
  META_NAME = "Evaluate Boolean expression"
  META_DESCRIPTION = "Evaluates a boolean logic operation between two operands"
  META_OPERANDS = {
    "x": {
      "name": "X Term",
      "description": "First term of expression",
      "valid_types": [bool]
    },
    "y": {
      "name": "Y Term",
      "description": "Second term of expression",
      "valid_types": [bool]
    }
  }
  META_ARGS = {
    "operation": {
      "name": "Operation to perform",
      "description": "Logical operation to be performed on operands x & y",
      "type": basestring,
      "options": {
        "AND": "Boolean AND",
        "OR": "Boolean OR"
      }
    },
    "true_value": {
      "name": "True value",
      "description": "Value to generate if expression evals True",
      "type": basestring
    },
    "false_value": {
      "name": "False value",
      "description": "Value to generate if expression evals False",
      "type": basestring
    }
  }

  def _evaluate(self):
    op = self.get_argument('operation').upper()

    if op == 'AND':
      if self.get_operand('x') and self.get_operand('y'):
        return self.get_argument('true_value')
      else:
        return self.get_argument('false_value')
    elif op == 'OR':
      if self.get_operand('x') or self.get_operand('y'):
        return self.get_argument('true_value')
      else:
        return self.get_argument('false_value')

  def guess_return_type(self):
    return basestring.__name__


class TextMatchModifier(FieldModifier):
  """ Performs basic text matching """
  META_NAME = "Match text"
  META_DESCRIPTION = "Performs basic text matching"
  META_OPERANDS = {
    "value": {
      "name": "Text value",
      "description": "Full text upon which matching will be attempted",
      "valid_types": [basestring]
    }
  }
  META_ARGS = {
    "operation": {
      "name": "Operation to perform",
      "description": "Type of match to perform",
      "type": basestring,
      "options": {
        "starts": "Starts with",
        "contains": "Contains",
        "ends": "Ends with",
      }
    },
    "needle": {
      "name": "Text to match",
      "description": "Text to match in value",
      "type": basestring
    },
    "true_value": {
      "name": "True value",
      "description": "Optional value to generate if match evals true, boolean True by default",
      "type": basestring
    },
    "false_value": {
      "name": "False value",
      "description": "Optional value to generate if expression evals False, boolean False by default",
      "type": basestring
    }
  }

  def _evaluate(self):
    op = self.get_argument('operation')
    needle = self.get_argument('needle')
    haystack = self.get_operand('value')
    trueval = self.get_argument('true_value', True)
    falseval = self.get_argument('false_value', False)

    if op == 'starts':
      return trueval if haystack.startswith(needle) else falseval
    elif op == 'ends':
      return trueval if haystack.endswith(needle) else falseval
    else:
      try:
        haystack.index(needle)
        return trueval
      except ValueError:
        return falseval

  def guess_return_type(self):
    return basestring.__name__


class ArithmeticModifier(FieldModifier):
  """ Performs Basic arithmetic operation on terms """

  META_NAME = "Evaluate basic arithmetic expression"
  META_DESCRIPTION = "Evaluates basic arithmetic operations between two operands"
  META_OPERANDS = dict()
  META_ARGS = {
    "expression": {
      "name": "expression to evaluate",
      "description": "Arithmetic expression to be evaluated",
      "type": basestring
    }
  }

  def _evaluate(self):
    expression = str(self.get_argument('expression'))
    operands = self.get_operands()
    parser = Parser()
    try:
      return parser.parse(expression).evaluate(operands)
    except TypeError:
      msg = "Type Error expression {} with operands {}"
      raise ValueError(msg.format(expression, operands))
    except ZeroDivisionError:
      msg = "Zero division for expression {} with operands {}"
      logging.warn(msg.format(expression, operands))
      return float('NaN')
    except Exception as e:
      msg = u"PyExpression error: {} for expression '{}' with opers: {}"
      logging.warn(msg.format(e, expression, operands))
      raise

  def guess_return_type(self):
    return float.__name__


class NdbKeyIdModifier(FieldModifier):
  """ Executes id() on ndb.Key properties """

  META_NAME = "Resolve ID from NDB Key object"
  META_DESCRIPTION = "Evaluates id() function on NDB.Key objects"
  META_OPERANDS = dict()
  META_ARGS = {}
  META_OPERANDS = {
    "value": {
      "name": "Property name",
      "description": "id() method will be run on provided attribute",
      "valid_types": [ndb.Key]
    }
  }

  def _evaluate(self):
    val = self.get_operand('value')
    if isinstance(val, ndb.Key):
      return val.id()
    else:
      try:
        key = ndb.Key(urlsafe=val)
        return key.id()
      except ProtocolBufferDecodeError:
        return None

  def guess_return_type(self):
    return long.__name__


class NdbQueryModifier(FieldModifier):
  """ Executes an ndb.Query() with given params """

  META_NAME = "Query Modifier"
  META_DESCRIPTION = "Evaluates an ndb.Query()"
  META_OPERANDS = dict()
  META_ARGS = {
    "namespace": {
      "name": "Namespace",
      "description": "Namespace over which query will be executed",
      "valid_types": [basestring]
    },
    "kind": {
      "name": "Kind",
      "description": "Entity Kind to query",
      "valid_types": [basestring]
    },
    "filters": {
      "name": "Filters",
      "description": "Filters to apply to query",
      "valid_types": [list]
    },
    "orders": {
      "name": "Order clauses",
      "description": "Order clauses to apply to query",
      "valid_types": [list]
    }
  }

  def _evaluate(self):
    namespace = self.get_argument('namespace')
    kind = self.get_argument('kind')
    raw_filters = self.get_argument('filters')
    raw_orders = self.get_argument('orders')

    if raw_filters:
      nodes = list()
      for f in raw_filters:
        try:
          fval = self.get_operand(f[2])
        except (NameError, KeyError):
          # user provided value if not resolved from operands
          fval = f[2]
        finally:
          nodes.append(ndb.query.FilterNode(f[0], f[1], fval))
          filters = ndb.query.ConjunctionNode(*nodes)
    else:
      filters = None

    if raw_orders:
      nodes = list()
      for o in raw_orders:
        if len(o) == 1:
          direc = datastore_query.PropertyOrder.ASCENDING
        elif o[1] == "ASC":
          direc = datastore_query.PropertyOrder.ASCENDING
        elif o[1] == "DESC":
          direc = datastore_query.PropertyOrder.DESCENDING

        nodes.append(datastore_query.PropertyOrder(o[0], direc))
      orders = datastore_query.CompositeOrder(nodes)
    else:
      orders = None

    q = ndb.Query(kind=kind, filters=filters, namespace=namespace, orders=orders)
    return q.get().to_dict()

  def guess_return_type(self):
    dict.__name__
