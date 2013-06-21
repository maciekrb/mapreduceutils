
import csv
from cStringIO import StringIO
from collections import OrderedDict
from google.appengine.ext import db
from mapreduce import context
from mapreduce.util import handler_for_name

def datastore_map(record):
  """
  Mapping function for Datastore entries
  @TODO assert all properties exist and are correct, throw errors if 
  additional unknown params are given as can be typos that could
  for example skip filtering
  """

  ctx = context.get()
  property_map = ctx.mapreduce_spec.mapper.params.get('property_map')

  map_rule = _get_mapping_entry(property_map, record)
  if map_rule and _record_matches_filters(record, map_rule.get("property_filters")):
    row = _get_mapped_properties(record, map_rule["property_list"])
    if row:
      yield (_to_csv(row))

def _get_mapping_entry(property_map, record):
  """
  Tries to match a property_match_name and property_match_value to the record

  Args:
    - property_map (list) list of rules for entry processing
    - record (db.Model, ndb.Model) Datastore entry to be processed

  Returns:
    the rule that will be used to process the record

  @TODO: Any optimization possibilities ??
  """
  for p in property_map:
    attr = _get_attribute_value(record, p["property_match_name"])
    if (attr == p["property_match_value"]):
      return p

def _record_matches_filters(record, property_filters):
  """
  Processes a filter list to determine if given record matches all the filters

  Provides an additional filtering mechanism so filters can be applied in the
  context of property_match_name and property_match_value. 

  If you need global filters (that match all records), you are better off
  using native mapreduce filters.

  Args:
    - record (db.Model, ndb.Model) Datastore entity instance
    - property_filters (tuple) Tuple formated in the following way:
      (attr_name, operation, value)
      - attr_name: (str) indicates the attribute that will be fetched
        via `getattr(record, attr_name)` from the record
      - operation: (str) indicates the comparison operation to perform.
        Currently only '=' (equalty) is supported.
      - value: an arbitrary value that will be matched against the value
        provided by `getattr(record, attr_name)`
  """
  if property_filters is None:
    return True

  for rule in property_filters:
    attr, oper, cmp_value = rule
    real_value = _get_attribute_value(record, attr)
    if oper == '=' and real_value != cmp_value:
      return False

  return True

def _get_mapped_properties(record, property_list):
  """
  Resolves the defined property list from the record 

  Args:

    - record: (db.Model, ndb.Model) Datastore model instance to be processed
    - property_list: List of fields to be generated from the record by applying
      the following rules:

      String arguments are treated as model attributes and resolved using getattr(). 
      Tuples define operations as follows:
        (assigned_name, resolve_function, function_args)
        - assigned name is a name mapped to the result, so it can be futher used
          in other operations.
        - resolve_function is an arbitrary function that will be passed the instance
          of the record followed by function_args as keyword args. The function should
          be defined as a qualified name some_python_module.class/function/method.
        - function_args: is a dictionary of keyword arguments to be passed to the
          resolve_function.

  If a property from property_list does not exist, a None type is added instead
  so column structure is maintained.

  Returns:
    Ordered Dictionary containing property_list items in their respective order
  """
  obj = []
  for k in property_list:
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

