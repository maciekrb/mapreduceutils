"""
Basice value modifier classes

Implements Basic format and logic modifiers
"""
import datetime
from datastoreutils.modifiers import FieldModifier

class DateModifier(FieldModifier):
  """
  Provides strftime for DateObjects
  """

  _META_NAME = 'Date Formatter'
  _META_DESCRIPTION = 'Date Formatter Description'
  _META_ARGS = {
    "date_format": {
      "name": "Output format",
      "description": "Desired Date Time field output format",
      "type": basestring,
      "options": {
        "%Y-%m-%d" : 'Year, month and day as decimal numbers',
        "%Y-%m" : 'Year and month as decimal numbers',
        "%Y-%W" : 'Year and week number as decimal numbers',
        "%a" : 'Weekday as locale abbreviated name (Sun, Mon)',
        "%A" : 'Weekday as locale full name (Sunday, Monday)',
        "%w" : 'Weekday as decimal number, where 0 is Sunday and 6 is Saturday',
        "%d" : 'Day of month as zero-padded decimal number',
        "%b" : 'Month as locale abbreviated name',
        "%B" : 'Month as locale full name',
        "%m" : 'Month as zero-padded decimal number',
        "%Y" : 'Year with century as decimal number',
        "%j" : 'Day of the year as a zero-padded decimal number',
        "%W" : 'Week number of the year (Monday as first day of week) as decimal number'
      }
    }
  }

  _META_OPERANDS = {
    "date": {
      "name": "Date column",
      "description": "The provided column value will be converted to the given date format",
      "valid_types": [datetime.datetime]
    }
  }

  def _evaluate(self):
    date_format = self.get_argument('date_format')
    f = getattr(self.get_operand('date'), 'strftime')
    return f(date_format)

class ConstantModifier(FieldModifier):
  """
  Provides constant value output
  """
  _META_NAME = "Constant Value"
  _META_DESCRIPTION = "Allows to generate constant values for a column"
  _META_ARGS = {
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
  _META_OPERANDS = {} # No operands are required for constant value generation

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

