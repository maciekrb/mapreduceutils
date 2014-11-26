"""
Modifier implementation
"""
from mapreduce.util import for_name

__all__ = ['FieldModifier', 'primitives']


class FieldModifier(object):

  def __init__(self, identifier, operands=None, arguments=None):
    self.identifier = identifier

    operands = operands or {}
    self.operands = {key: val for key, val in operands.iteritems()}

    arguments = arguments or {}
    self.arguments = {key: val for key, val in arguments.iteritems()}

  def args_are_valid(self):
    pass

  def operands_are_valid(self):
    pass

  def get_argument(self, name, default=None):
    """ Retrieves a registred argument by it's name """

    return self.arguments.get(name, default)

  def get_operand(self, name):
    """ retrieves a registered operand by it's name """

    value = self.operands[name]
    if (isinstance(value, basestring)
       and ('model.' in value or 'identifier.' in value)):

      prefix, attr_name = self.operands[name].split('.')
      if prefix == 'model':
        return getattr(self.record, attr_name)
      elif prefix == 'identifier':
        return self.get_value_from_chain(attr_name)

      raise NameError('Dont know how to obtain scope "{}"'.format(prefix))
    return value

  def get_operands(self):
    return {opname: self.get_operand(opname) for opname in
            self.operands.keys()}

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
    modifier_chain[self.identifier] = self._evaluate()

  def get_value_from_chain(self, identifier):
    """
    Retrieves a value from a previous modifier in the chain

    Args:
      identifier: (str) identifier of the previous modifier

    Returns:
      value assigned by the FieldModifier
    """
    return self.modifier_chain[identifier]

  def to_dict(self):
    """
    Retrieves a serialized version of the Modifier
    """
    return {
      "method": "{}.{}".format(
        self.__class__.__module__,
        self.__class__.__name__),
      "identifier": self.identifier,
      "operands": {k: v for k, v in self.operands.iteritems()},
      "args": {k: v for k, v in self.arguments.iteritems()}
    }

  @classmethod
  def from_qualified_name(cls, qualified_name, constructor_args={}, prefix=''):
    """
    Evaluates a qualified name in order to get a Modifier class or instance

    If the qualified name exists, and the constructor arguments are provided,
    an instance of the FieldModifer is returned. If no constructor args are
    provided the class is returned instead
    Args:
      - record (db.Model or ndb.Model) Datastore Entity instance
      - qualified_name (str) string with a qualified name to evaluate
      - args (dict) dictionary of keyword args to use for evaluation

    Returns:
      A class or an instance of the FieldModifier specified by the qualified
      name and prefix.

    """
    path = "{prefix}.{qn}".format(
      prefix=prefix,
      qn=qualified_name
    ) if prefix else qualified_name

    class_obj = for_name(path)
    if not issubclass(class_obj, cls):
      msg = "Name {} does not resolve into a FieldModifier class"
      raise ValueError(msg.format(path))

    if constructor_args:
      return class_obj(**constructor_args)
    else:
      return class_obj

  @classmethod
  def from_dict(cls, definition):
    """
    Instances a FieldModifier from a dict definition

    Args:
      - definition: (dict) containing the following keys:
        - method: (str) qualified name of the concrete modifier
        (datastoreutils.primitives.DateFormatModifier)
        - operands: (dict) optional dictionary of operands
        - args: (dict) optional dictionary of arguments
    Returns:
      An instance of the concrete FileModifier i.e DateModifier
    """

    if 'method' not in definition:
      # support property rename when no method is defined
      definition['method'] = __name__ + ".BypassModifier"

    args = {
      'identifier': definition['identifier'],
      'arguments': definition.get('args'),
      'operands': definition.get('operands')
    }
    # @TODO: cache this
    obj = cls.from_qualified_name(
      definition['method'],
      constructor_args=args
    )
    return obj


class BypassModifier(FieldModifier):
  """ Renames a property to it's identifier """
  def _evaluate(self):
    return self.get_operand('value')
