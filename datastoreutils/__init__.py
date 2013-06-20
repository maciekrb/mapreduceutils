
import csv
from cStringIO import StringIO
from collections import OrderedDict
from google.appengine.ext import db
from mapreduce import context
from mapreduce.util import handler_for_name

def datastore_map(record):
  """
  Mapping function for Datastore entries
  """

  ctx = context.get()
  property_map = ctx.mapreduce_spec.mapper.params.get('property_map')

  row = _get_mapped_properties(property_map, record)
  if row:
    yield (_to_csv(row))

def _get_mapped_properties(property_map, record):
  """
  Resolves mapped properties according to property_map 

  Tries to match property_match_name as a property_name of the record. If
  a match exists, then compares the property_match_value. If value matches
  record's value, then the property_list resolution is attempted. First matched 
  combination is applied.

  If a property from property_list does not exist, a None type is added instead
  so column structure is maintained.
  """
  for p in property_map:
    attr = _get_attribute_value(record, p["property_match_name"])
    if (attr == p["property_match_value"]):
      obj = []
      for k in p["property_list"]:
        """ 
        property list can either have attribute names or tuples defining an attribute
        that will be created by a class/function/method qualified names

        Attribute names are defined as strings, qualified names should be tuples,
        first item defines the attribute to which it will be mapped, the second
        should define a qualified name such some_python_module.funcname
        """
        if isinstance(k, basestring):
          obj.append((k, _get_attribute_value(record, k)))
        else:
          p_key, qualified_name, args = k
          obj.append((p_key, _evaluate_name(record, qualified_name, args)))

      return OrderedDict(obj)

def _get_attribute_value(record, attr):
  """
  Resolves the value of the attribute ensuring a valid type is returned
  Args:
    - record (db.Model or ndb.Model) a Model instance
    - attr (str) name of attribute to fetch value for
  Returns:
    The value of the given attribute provided by the record instance
  """
  val = getattr(record, attr, None)

  if isinstance(val, basestring):
    return _encode(val)
  elif isinstance(val, db.Model):
    return str(val.key())
  else:
    return val
      
def _evaluate_name(record, qualified_name, args):
  """ 
  Evaluates a qualified name in order to get a value from a computation 

  Evaluated function must return a single value, which will be used as the
  value for the given column attribute

  Args:
    - record (db.Model or ndb.Model) Datastore Entity instance
    - qualified_name (str) string with a qualified name to evaluate
    - args (dict) dictionary of keyword args to use for evaluation

  Returns:
    mixed type value for column attribute
  
  """
  func = handler_for_name(qualified_name)
  return func(record=record, **args)

def _to_csv(data_obj):
  """ 
  Converts given fields to correctly formated CSV record 

  Uses Python csv library to create proper CSV record from given list
  Args:
    - field_list: (iterable) listo or ordered dict containing the data that should be converted
      to CSV

  Returns:
    String with correctly formated CSV record, including line terminating 
    characters.

  @TODO: add a param to spec other dialects
  """

  data = StringIO()
  writer = csv.writer(data)
  writer.writerow(data_obj.values() if isinstance(data_obj, OrderedDict) else data_obj )
  return data.getvalue()

def _encode(value):
  """ Enforces correct encoidng before generating a value """

  if isinstance(value, basestring):
    return value.encode('utf-8')
  else:
    return value

