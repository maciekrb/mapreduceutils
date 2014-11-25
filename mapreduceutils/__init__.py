from collections import OrderedDict
from copy import copy
from mapreduceutils.modifiers import FieldModifier
from mapreduceutils.propertymap import (
  KeyModelMatchRule,
  ModelRuleSet,
  PropertyMap
)
from mapreduceutils.writers import OutputWriter
from google.appengine.ext import (
  db,
  ndb
)
import json
import logging
from mapreduce import context

__all__ = [
  "PropertyMap", "KeyModelMatchRule", "ModelRuleSet", "FieldModifier",
  "record_map", "OutputWriter"
]


def cached_property(f):
  """returns a cached property that is calculated by function f"""
  def get(self):
    try:
      return self._property_cache[f]
    except AttributeError:
      self._property_cache = {}
      x = self._property_cache[f] = f(self)
      return x
    except KeyError:
      x = self._property_cache[f] = f(self)
      return x

  return property(get)


class MapperRecord(object):

  def __init__(self, input_obj):
    self._data = input_obj

  @classmethod
  def create(cls, input_obj):

    if isinstance(input_obj, ndb.Model):
      record = GAE_NDBRecord(input_obj)

    elif isinstance(input_obj, db.Model):
      record = GAE_DBRecord(input_obj)

    elif isinstance(input_obj, basestring):
      try:
        record = MapperRecord(json.loads(input_obj))
      except ValueError:
        logging.warn(u"Unable to load json: {}".format(input_obj))
        record = None

    return record

  def __getattr__(self, name):
      return self._data.get(name)

  def mapper_key(self, mapper_spec):
    props = self.pick_properties(mapper_spec)
    values = [unicode(f) for f in props.values()]
    return u"|".join(values)

  def matches_filters(self, property_filters=None, key_filters=None):
    """
    Processes a filter list to determine if given record matches all
    the filters

    Provides an additional filtering mechanism so filters can be applied
    to a model matching model_match_rule and thus generate a subset of
    records matched by the match rule.

    If key_filters are defined, these are processed before property filters.

    Args:
      - property_filters (iterable) List of tuples formated in the following
      way:

        (attr_name, operation, value)
        - attr_name: (str) indicates the attribute that will be fetched
          via `getattr(record, attr_name)` from the record
        - operation: (str) indicates the comparison operation to perform.
          Currently only '=' (equalty) is supported.
        - value: an arbitrary value that will be matched against the value
          provided by `getattr(record, attr_name)`

      - key_filters (iterable) List of lists of tuples formated in the
      following way:
        (Model name, Value).  Items in the list are considered paths conformed
        by the tuples they contain. Paths are processed left to right,
        ((ModelA, 1), (ModelB, 1)) will generate all records matching the key
        at the leftmost position ej:
          ((ModelA, 1), (ModelB,1), (ModelC, 3)), in general
          ((ModelA, 1), (ModelB,1), *).

        Every item of the list is considered a path, and evaluated as OR rule,
        if any path matches the record is generated.

    """
    if property_filters is None and key_filters is None:
      return True

    if not self.matches_key_filters(key_filters):
      return False

    if not self.matches_property_filters(property_filters):
      return False

    return True

  def matches_key_filters(self, key_filters):
    """
    Performs key filters validation on a record

    key filters should be a list of lists, if a record key
    matches any of the provided paths, it validates as true
    """

    if not key_filters:
      return True

    if not hasattr(self, '_key_pairs'):
      return True

    for rule_pair in key_filters:
      chain = list()
      for pos, r in enumerate(rule_pair):
        try:
          chain.append(self._key_pairs[pos] == tuple(r))
        except ValueError:
          return False

      if all(chain):
        return True

    return False

  def matches_property_filters(self, property_filters):
    """ Verifies if property filters match """

    if not property_filters:
      return True

    for rule in property_filters:
      attr, oper, cmp_value = rule
      oper = str(oper)
      real_value = getattr(self, attr)

      #logging.error("{},{},{}=={}".format(attr, oper, cmp_value, real_value))
      if oper == '=' and real_value != cmp_value:
        return False
      elif oper == 'IN' and real_value not in cmp_value:
        return False

    return True

  def pick_properties(self, property_list, defaults=None):
    """
    Resolves the defined property list from the record

    Args:
      - property_list: List of fields to be generated from the record by
        applying the following rules:

        String arguments are treated as model attributes and resolved using
        getattr().
        Tuples define operations as follows:
          (assigned_name, resolve_function, function_args)
          - assigned name is a name mapped to the result, so it can be futher
            used in other operations.
          - resolve_function is an arbitrary function that will be passed the
            instance of the record followed by function_args as keyword args.
            The function should be defined as a qualified name
            some_python_module.class/function/method.
          - function_args: is a dictionary of keyword arguments to be passed to
            the resolve_function.

    If a property from property_list does not exist, a None type is added
    instead so column structure is maintained.

    If defaults dict is defined, de provided value is used instead of None

    Returns:
      Ordered Dictionary containing property_list items
    """
    if defaults is None:
      defaults = dict()

    obj = OrderedDict()
    for k in property_list:
      if isinstance(k, basestring):
        obj[k] = getattr(self, k, defaults.get(k))
      elif isinstance(k, list):  # List of Modifier definitions
        modifier_chain = copy(obj)  # copy previously resolved attrs
        for mod_def in k:
          mod = FieldModifier.from_dict(mod_def)
          mod.eval(self, modifier_chain)
        obj[mod_def['identifier']] = modifier_chain[mod_def['identifier']]

    return obj

  def match_rule(self, property_map):
    """
    Tries to match one of the rules in property_map to the MapperRecord

    Args:
      - property_map (list) list of rules for entry processing
    Returns:
      the rule that will be used to process the record

    @TODO: Any optimization possibilities ??
    """
    for rule in property_map:
      if "model_match_rule" not in rule:
        raise KeyError("model_match_rule is not defined in {}".format(rule))

      if ("properties" not in rule["model_match_rule"]
         and "key" not in rule["model_match_rule"]):
        msg = "model_match_rule needs either properties or key defined {}"
        raise KeyError(msg.format(rule))

      match_rule = rule["model_match_rule"]
      key_match = False
      prop_match = False
      try:
        key_match = self.matches_key_filters([match_rule["key"]])
      except KeyError:  # Means no key was given to match
        key_match = True

      try:
        chain = list()
        for prop in match_rule["properties"]:
          rule_attr_name, rule_attr_value = prop
          attr_value = getattr(self, rule_attr_name)
          chain.append(attr_value == rule_attr_value)
        prop_match = all(chain)
      except KeyError:  # Means no properties were given to match
        prop_match = True

      if all((key_match, prop_match)):
        return rule


class GAE_DBRecord(MapperRecord):
  def __init__(self, input_obj):
    self._data = input_obj
    self._key = ndb.Key.from_old_key(self._data.key())
    self._key_pairs = self._key.pairs()

  def __getattr__(self, name):

    if name == 'key':
      value = self._key.urlsafe()

    else:
      raw_value = self._data._properties.get(name)
      if isinstance(raw_value, db.ReferenceProperty):
        """ We only want to compare the the key without de-referencing it """
        attr_key = "_{}".format(name)
        value = None
        if attr_key in self._data.__dict__:
          new_key = ndb.Key.from_old_key(self._data.__dict__.get(attr_key))
          value = new_key.urlsafe()
      else:
        value = getattr(self._data, name)

    return value


class GAE_NDBRecord(MapperRecord):
  def __init__(self, input_obj):
    self._data = input_obj
    self._key = input_obj.key
    self._key_pairs = input_obj.key.pairs()

  def __getattr__(self, name):

    value = getattr(self._data, name, None)
    if isinstance(value, ndb.Key):
      value = value.urlsafe()

    return value


class CSVRecord(MapperRecord):
  def __init__(self, input_obj, mapping_obj):
    msg = "Sorry :( CSV Records have not been implemented yet !"
    raise NotImplemented(msg)


def record_map(data_record):
  """
  Universal mapping function

  Accepts JSON, ndb.Model and db.Model instances as data and yields
  either tuples (for mapreduce) or records (map only) according
  to given property map and output format.

  Args:
    - data_record: db.Model, ndb.Model or string (JSON object)
  """

  ctx = context.get()
  property_map = ctx.mapreduce_spec.mapper.params.get('property_map')
  output_format = ctx.mapreduce_spec.mapper.params.get('output_format', 'JSON')
  writer_args = ctx.mapreduce_spec.mapper.params.get('writer_args', dict())

  record = MapperRecord.create(data_record)
  if record:
    map_rule = record.match_rule(property_map)
    if map_rule and record.matches_filters(
        property_filters=map_rule.get("property_filters"),
        key_filters=map_rule.get("key_filters")
      ):
      try:
        row = record.pick_properties(map_rule["property_list"], map_rule.get('defaults'))
        if any(row.values()):
          writer = OutputWriter.get_writer(output_format)
          if 'mapper_key_spec' in map_rule:
            key = record.mapper_key(map_rule.get('mapper_key_spec'))
            data = writer.write(row, **writer_args)
            #logging.warn("Mapper pre yield MR: {}:{}".format(key, data))
            yield (key, data)
          else:
            data = writer.write(row)
            #logging.warn("Mapper pre yield M: {}".format(data))
            yield data
      except ValueError as m:
        logging.warn("Skipping record due to modifier errors:{}".format(m))
