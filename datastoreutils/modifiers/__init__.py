"""
Modifier implementation
"""
from mapreduce.util import for_name

__all__ = ['FieldModifier', 'primitives']


class FieldModifier(object):

  def __init__(self, identifier, operands=None, arguments=None):

    self.identifier = identifier

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
