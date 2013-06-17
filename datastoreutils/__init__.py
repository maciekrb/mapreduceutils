
import csv
from cStringIO import StringIO
from collections import OrderedDict
from google.appengine.ext import db
from mapreduce import context

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
    if (hasattr(record, p["property_match_name"]) and 
        getattr(record, p["property_match_name"]) == p["property_match_value"]):
      obj = []
      for k in p["property_list"]:
        obj.append((k,getattr(record,k)) if hasattr(record, k) else None)
      return OrderedDict(obj)
      
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

