import csv
from cStringIO import StringIO
from google.appengine.ext import db, ndb
from mapreduce import context
from datastoreutils.modifiers import FieldModifier

__all__ = [ "FieldModifier", "record_map", "get_attribute_value", "to_csv" ]

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

class DatastoreRecord(object):
  def __init__(self, datastore_entry):
    """
    Creates a DatastorRecord object adding indirection to db and ndb models
    Args:
      -
    """
    self.datastore_entry = datastore_entry

  @property
  def key(self):
    if isinstance(self.datastore_entry, db.Model):
      return ndb.Key.from_old_key(self.datastore_entry.key())
    elif isinstance(self.datastore_entry, ndb.Model):
      return self.datastore_entry.key
    else:
      raise ValueError("Unexpected instance of {} in {}, db, or ndb model instance expected"
          .format(type(self.datastore_entry), self.__class__.__name__))

  @property
  def key_pairs(self):
    return self.key.pairs()

  def __getattr__(self, name):
    """
    Resolves the value of the attribute ensuring a valid type is returned
    Args:
      - name: (str) name of attribute to be retrieved upon the model
    Returns:
      The value of the given attribute provided by the record instance
    """

    if isinstance(self.datastore_entry, db.Model):
      prop = self.datastore_entry._properties.get(name)
      if isinstance(prop, db.ReferenceProperty):
        """ We only want to compare the the key without de-referencing it """
        attr_key = "_{}".format(name)
        val = None
        if attr_key in self.datastore_entry.__dict__:
          val = str(self.datastore_entry.__dict__.get(attr_key))
      else:
        val = getattr(self.datastore_entry, name, None)

    elif isinstance(self.datastore_entry, ndb.Model):
      val = getattr(self.datastore_entry, name, None)

    elif isinstance(self.datastore_entry, dict):
      raise Exception("Unimplemented record of type dict")

    else:
      raise TypeError("Unknown record type, either Datastore db or ndb models or text CVS records are supported")

    if isinstance(val, basestring):
      return val.encode('utf8')
    else:
      return val

  def matches_filters(self, property_filters, key_filters=None):
    """
    Processes a filter list to determine if given record matches all the filters

    Provides an additional filtering mechanism so filters can be applied to a model
    matching model_match_rule.

    If you need global filters (that match all records), you are better off
    using native mapreduce filters.

    If key_filters are defined, these are processed before property filters.

    Args:
      - property_filters (iterable) List of tuples formated in the following way:
        (attr_name, operation, value)
        - attr_name: (str) indicates the attribute that will be fetched
          via `getattr(record, attr_name)` from the record
        - operation: (str) indicates the comparison operation to perform.
          Currently only '=' (equalty) is supported.
        - value: an arbitrary value that will be matched against the value
          provided by `getattr(record, attr_name)`
      - key_filters (iterable) List of tuples formated in the following way:
        (Model name, Value). Key filters are processed left to right, meaning that
        the order of key filters provided does matter. In only one key filter is provided
        it is evaluated against the left most pair of the key and so on.
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

    for rule_pair in key_filters:
      chain = list()
      for pos, r  in enumerate(rule_pair):
        chain.append(self.key_pairs[pos] == r)

      if all(chain):
        return True

    return False

  def matches_property_filters(self, property_filters):
    """ Verifies if property filters match """
    for rule in property_filters:
      attr, oper, cmp_value = rule
      oper = str(oper)
      real_value = getattr(self, attr)
      if oper == '=' and real_value != cmp_value:
        return False
      elif oper == 'IN' and real_value not in cmp_value:
        return False

    return True

  def pick_properties(self, property_list):
    """
    Resolves the defined property list from the record

    Args:
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
        obj.append(getattr(self, k))
      elif isinstance(k, list): # List of Modifier definitions
        modifier_chain = {}
        for mod_def in k:
          mod = FieldModifier.from_dict(mod_def)
          mod.eval(self, modifier_chain)
        obj.append(modifier_chain[mod_def['identifier']])

    return obj

  def match_rule(self, property_map):
    """
    Tries to match one of the rules in property_map to the datastore_record

    Args:
      - property_map (list) list of rules for entry processing
    Returns:
      the rule that will be used to process the record

    @TODO: Any optimization possibilities ??
    """
    for rule in property_map:
      if "model_match_rule" not in rule:
        raise KeyError("model_match_rule is not defined in {}".format(rule))

      if "properties" not in rule["model_match_rule"] and "key" not in rule["model_match_rule"]:
        raise KeyError("model_match_rule needs either properties or key defined {}".format(rule))

      match_rule = rule["model_match_rule"]
      key_match = False
      prop_match = False
      try:
        key_match = self.matches_key_filters([match_rule["key"]])
      except KeyError: # Means no key was given to match
        key_match = True

      try:
        chain = list()
        for prop in match_rule["properties"]:
          rule_attr_name, rule_attr_value = prop
          attr_value = getattr(self, rule_attr_name)
          chain.append(attr_value == rule_attr_value)
        prop_match = all(chain)
      except KeyError: # Means no properties were given to match
        prop_match = True

      if all((key_match, prop_match)):
        return rule

def record_map(datastore_record):
  """
  Mapping function for Datastore entries
  @TODO assert all properties exist and are correct, throw errors if
  additional unknown params are given as can be typos that could
  for example skip filtering
  """

  ctx = context.get()
  property_map = ctx.mapreduce_spec.mapper.params.get('property_map')

  record = DatastoreRecord(datastore_record)
  map_rule = record.match_rule(property_map)
  if map_rule and record.matches_filters(map_rule.get("property_filters"),
      map_rule.get("key_filters")):
    row = record.pick_properties(map_rule["property_list"])
    if any(row):
      yield (to_csv(row))

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

