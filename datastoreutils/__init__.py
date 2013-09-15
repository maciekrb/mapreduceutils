
import csv
import logging
from cStringIO import StringIO
from collections import OrderedDict
from google.appengine.ext import db, ndb
from mapreduce import context
from mapreduce.util import for_name

__all__ = [ "FieldModifier", "record_map", "get_attribute_value", "to_csv" ]


class FieldModifier(object):

  def __init__(self, identifier=None, operands=None, arguments=None):

    self.identifier = identifier
    self.meta_name = self._META_NAME
    self.meta_description = self._META_DESCRIPTION
    self.meta_arguments = self._META_ARGS
    self.meta_operands = self._META_OPERANDS

    if operands:
      self.operands = { key : val for key, val in operands.iteritems() }

    if arguments:
      self.arguments = { key : val for key, val in arguments.iteritems() }

  def args_are_valid(self):
    pass

  def operands_are_valid(self):
    pass

  def get_argument(self, name):
    return self.arguments[name]

  def get_operand(self, name):
    prefix, attr_name = self.operands[name].split('.')
    if prefix == 'model':
      return getattr(self.record, attr_name)
    elif prefix == 'identifier':
      return self.get_value_from_chain(attr_name)

    raise NameError('Dont know how to obtain scope "{}"'.format(prefix))

  def eval(self, record, modifier_chain):
    """
    Evaluates the modifier

    Args:
      - record: (Model) model object
      - modifier_chain: (dict) dictionary containing previous modifiers from
        the chain
    """
    self.record = record
    self.modifier_chain = modifier_chain
    modifier_chain[self.identifier] = self._evaluate(record)

  def get_value_from_chain(self, identifier):
    """
    Retrieves a value from a previous modifier in the chain

    Args:
      identifier: (str) identifier of the previous modifier

    Returns:
      value assigned by the FieldModifier
    """
    return self.modifier_chain[identifier]

  @classmethod
  def from_qualified_name(cls, qualified_name, constructor_args=None, prefix=''):
    """
    Evaluates a qualified name in order to get a Modifier class

    Args:
      - record (db.Model or ndb.Model) Datastore Entity instance
      - qualified_name (str) string with a qualified name to evaluate
      - args (dict) dictionary of keyword args to use for evaluation

    Returns:
      mixed type value for column attribute

    """
    path = "{prefix}.{qn}".format(prefix=prefix, qn=qualified_name) if prefix else qualified_name
    class_obj = for_name(path)
    if not issubclass(class_obj, cls):
      raise ValueError("Name {} does not resolve into a FieldModifier class".format(path))

    return class_obj(**constructor_args)

  @classmethod
  def from_dict(cls, definition):

    args = {
      'identifier': definition['identifier'],
      'arguments': definition.get('args'),
      'operands': definition.get('operands')
    }
    obj = cls.from_qualified_name(definition['method'], constructor_args=args)
    return obj

def record_map(record):
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
      yield (to_csv(row))

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
    attr = get_attribute_value(record, p["property_match_name"])
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
    oper = str(oper)
    real_value = get_attribute_value(record, attr)
    if oper == '=' and real_value != cmp_value:
      return False
    elif oper == 'IN' and real_value not in cmp_value:
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
      obj.append(get_attribute_value(record, k))
    elif isinstance(k, list): # List of Modifier definitions
      modifier_chain = {}
      for mod_def in k:
        mod = FieldModifier.from_dict(mod_def)
        mod.eval(record, modifier_chain)
      obj.append(modifier_chain[mod_def['identifier']])

  return obj

def get_attribute_value(record, attr):
  """
  Resolves the value of the attribute ensuring a valid type is returned
  Args:
    - record (db.Model or ndb.Model) a Model instance
    - attr (str) name of attribute to fetch value for
  Returns:
    The value of the given attribute provided by the record instance
  """
  if isinstance(record, db.Model):
    prop = record._properties.get(attr)
    if isinstance(prop, db.ReferenceProperty):
      """ We only want to compare the the key without de-referencing it """
      attr_key = "_%s" % attr
      val = str(record.__dict__.get(attr_key)) if attr_key in record.__dict__ else None
    else:
      val = getattr(record, attr, None)

  elif isinstance(record, ndb.Model):
    val = getattr(record, attr, None)

  elif isinstance(record, dict):
    raise Exception("Unimplemented record of type dict")

  else:
    raise TypeError("Unknown record type, either Datastore db or ndb models or text CVS records are supported")

  if isinstance(val, basestring):
    return _encode(val)
  else:
    return val

def to_csv(data_obj):
  """
  Converts given fields to correctly formated CSV record

  Uses Python csv library to create proper CSV record from given list
  Args:
    - field_list: (iterable) list containing the data that should be converted
      to CSV

  Returns:
    String with correctly formated CSV record, including line terminating
    characters.

  @TODO: add a param to spec other dialects
  """

  data = StringIO()
  writer = csv.writer(data)
  writer.writerow(data_obj)
  return data.getvalue()

def _encode(value):
  """ Enforces correct encoidng before generating a value """

  if isinstance(value, basestring):
    return value.encode('utf-8')
  else:
    return value

