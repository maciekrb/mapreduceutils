"""
Basice value modifier classes

Implements Basic format and logic modifiers
"""
import datetime
from . import FieldModifier
from math import ceil, floor


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
    }
  }

  def _evaluate(self):
    date_format = self.get_argument('date_format')
    f = getattr(self.get_operand('value'), 'strftime')
    return f(date_format)

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
    secs = res.days*86400 + res.seconds

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

    return abs(out) if absolute == True else out

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

class CoerceToNumberModifier(FieldModifier):
  """ Coerces a string/float into an int, so operations can be performed with values """

  META_NAME = 'Convert a string to an Integer'
  META_DESCRIPTION = 'Coerces a string or float to an integer value, so it can be used to perform integer operations'
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
        out = int(number) if  out_type == "int" else number

      if isinstance(number, int):
        out = float(number) if  out_type == "float" else number

      if isinstance(number, basestring):
        if out_type == "int":
          out = int(number)
        elif out_type == "float":
          out = float(number)

    except ValueError as e:
      out = e.message

    finally:
      return out

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
    return round(number, ndigits)

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

