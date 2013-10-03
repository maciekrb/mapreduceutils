"""
Basice value modifier classes

Implements Basic format and logic modifiers
"""
import datetime
from . import FieldModifier
from math import ceil, floor


class DateModifier(FieldModifier):
  """
  Provides strftime for DateObjects
  """

  META_NAME = 'Date Formatter'
  META_DESCRIPTION = 'Date Formatter Description'
  META_OPERANDS = {
    "date_input": {
      "name": "Date column",
      "description": "The provided column value will be converted to the given date format",
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
    f = getattr(self.get_operand('date_input'), 'strftime')
    return f(date_format)


class ConstantModifier(FieldModifier):
  """
  Provides constant value output
  """
  META_NAME = "Constant Value"
  META_DESCRIPTION = "Allows to generate constant values for a column"
  META_OPERANDS = {}  # No operands are required for constant value generation
  META_ARGS = {
    "const_value": {
      "name": "Constant value",
      "description": "The constant value that should be generated",
      "type": basestring
    },
    "const_type": {
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
    vtype = self.get_argument('const_type')
    value = self.get_argument('const_value')
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


class RoundModifier(FieldModifier):
  META_NAME = 'Round float'
  META_DESCRIPTION = 'Round float description'
  META_OPERANDS = {
    "round_input": {
      "name": "Float column",
      "description": None,
      "valid_types": [float]
    }
  }
  META_ARGS = {
    "round_ndigits": {
      "name": "Number of decimal places",
      "description": None,
      "type": int
    }
  }

  def _evaluate(self):
    ndigits = self.get_argument('round_ndigits')
    number = self.get_operand('round_input')
    return round(number, ndigits)


class ParseIntModifier(FieldModifier):
  META_NAME = 'Parse to integer'
  META_DESCRIPTION = 'Parse to integer description'
  META_OPERANDS = {
    "parseint_input": {
      "name": "Float column",
      "description": None,
      "valid_types": [float]
    }
  }
  META_ARGS = {
    "parseint_type": {
      "name": "Type of parsing",
      "description": None,
      "type": basestring,
      "options": {
        "round": "Round",
        "ceil": "Ceil",
        "floor": "Floor"
      }
    }
  }

  def _evaluate(self):
    # @TODO: en caso de error, que hay que hacer? generar excepcion?
    func = self.get_argument('parseint_type')
    number = self.get_operand('parseint_input')
    if func == 'round':
      new_value = round(number)
    elif func == 'ceil':
      new_value = ceil(number)
    elif func == 'floor':
      new_value = floor(number)
    else:
      new_value = ''  # @TODO: devolver string vacio o el numero original ("number")
    return new_value


class DatetimeOperationModifier(FieldModifier):
  META_NAME = 'Sum of dates'
  META_DESCRIPTION = None
  META_OPERANDS = {
    "dateoperation_input": {
      "name": "Date/datetime column",
      "description": None,
      "valid_types": [datetime.date, datetime.datetime]
    }
  }
  META_ARGS = {
    "dateoperation_value": {
      "name": "Value",
      "description": None,
      "type": int
    },
    "dateoperation_unit": {
      "name": "Units",
      "description": None,
      "type": basestring,
      "options": {
        "years": "Years",
        "months": "Months",
        "days": "Days",
        "hours": "Hours",
        "minutes": "Minutes",
        "seconds": "Seconds"
      }
    }
  }

  def _evaluate(self):
    # @TODO: en caso de error, que hay que hacer? generar excepcion?
    func = self.get_argument('parseint_type')
    number = self.get_operand('parseint_input')
    if func == 'round':
      new_value = round(number)
    elif func == 'ceil':
      new_value = ceil(number)
    elif func == 'floor':
      new_value = floor(number)
    else:
      new_value = ''  # @TODO: devolver string vacio o el numero original ("number")
    return new_value


class DatetimeDiffModifier(FieldModifier):
  META_NAME = 'Date difference'
  META_DESCRIPTION = None
  META_OPERANDS = {
    "datediff_input1": {
      "name": "Date/datetime 1",
      "description": None,
      "valid_types": [datetime.date, datetime.datetime]
    },
    "datediff_input2": {
      "name": "Date/datetime 2",
      "description": None,
      "valid_types": [datetime.date, datetime.datetime]
    }
  }
  META_ARGS = {
    "datediff_result_format": {
      "name": "Result format",
      "description": None,
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
    "dateoperation_absolute_value": {
      "name": "Absolute value",
      "description": None,
      "type": bool
    }
  }

  def _evaluate(self):
    #math.fabs(x): Return the absolute value of x.
    return 'foo'
