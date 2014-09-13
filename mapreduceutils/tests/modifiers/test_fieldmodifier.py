import unittest
from mapreduceutils.modifiers import FieldModifier

class DummyFieldModifier(FieldModifier):
  META_NAME = "Test Name"
  META_DESCRIPTION = "Test Description"
  META_ARGS = {
    "arg_1": {
      "name": "argument 1",
      "description": "argument 1 desc",
      "type": basestring,
      "options": {
        "a": "option a",
        "b": "option b",
        }
    },
    "arg_2": {
      "name": "argument 2",
      "description": "argument 2 desc",
      "type": basestring,
    }
  }
  META_OPERANDS = {
    "operand_1": {
      "name": "operand 1",
      "description": "operand 1 desc",
      "valid_types": ["IntegerProperty"]
    }
  }

  def init(self):
    pass

  def _evaluate(self):
    return 'testvalue'


class DummyFieldModifier2(FieldModifier):
  META_NAME = "Test Name"
  META_DESCRIPTION = "Test Description"
  META_ARGS = {
    "arg_1": {
      "name": "argument 1",
      "description": "argument 1 desc",
      "type": basestring,
      "options": {
        "a": "option a",
        "b": "option b",
        }
    },
    "arg_2": {
      "name": "argument 2",
      "description": "argument 2 desc",
      "type": basestring,
    }
  }
  META_OPERANDS = {
    "operand_1": {
      "name": "operand 1",
      "description": "operand 1 desc",
      "valid_types": [basestring]
    }
  }

  def init(self):
    pass

  def _evaluate(self):
    previous_operation_value = self.get_operand('operand_1')
    if previous_operation_value == 'testvalue':
      return 'OKI'
    else:
      return 'FAIL'


class DummyModel(object):
  pass


class TestFieldModifier(unittest.TestCase):

  def test_metadata_definition(self):
    """ FieldModifier Class metadata correctly defined """

    self.assertEqual(DummyFieldModifier.META_NAME, "Test Name")
    self.assertEqual(DummyFieldModifier.META_DESCRIPTION, "Test Description")
    self.assertIsInstance(DummyFieldModifier.META_ARGS, dict)
    self.assertIsInstance(DummyFieldModifier.META_OPERANDS, dict)

  def test_instantiation_from_dict(self):
    """ FieldModifier instantiation from dict  """

    definition = {
      "identifier": "xy0002",
      "method": "mapreduceutils.tests.modifiers.test_fieldmodifier.DummyFieldModifier",
      "args": {
        "test_arg1": "abc",
        "test_arg2": "bcd"
      },
      "operands": {
        "operand1": "model.prop_a",
        "operand2": "identifier.xxxa1"
      }
    }

    mod = FieldModifier.from_dict(definition)
    self.assertIsInstance(mod, DummyFieldModifier)
    self.assertEquals('abc', mod.get_argument('test_arg1'))
    self.assertEquals('bcd', mod.get_argument('test_arg2'))

    rec = DummyModel()
    rec.prop_a = "123"

    mod.eval(rec, {'xxxa1': '234'})
    self.assertEquals("123", mod.get_operand('operand1'))
    self.assertEquals("234", mod.get_operand('operand2'))

  def test_to_dict(self):
    """ FieldModifier creates consistent to_dict representation  """

    definition = {
      "identifier": "xy0002",
      "method": "mapreduceutils.tests.modifiers.test_fieldmodifier.DummyFieldModifier",
      "args": {
        "test_arg1": "abc",
        "test_arg2": "bcd"
      },
      "operands": {
        "operand1": "model.prop_a",
        "operand2": "identifier.xxxa1"
      }
    }

    mod = FieldModifier.from_dict(definition)
    self.assertEqual(definition, mod.to_dict())

  def test_modifier_return_value(self):
    """ FieldModifier class returns expected values """

    chain = {}
    record = DummyModel()
    modifier = DummyFieldModifier(identifier='xy0001')
    modifier.eval(record, chain)
    self.assertEqual('testvalue', chain['xy0001'])

  def test_modifier_chaining(self):
    """ FieldModifer chained operations work """

    chain = {}
    record = DummyModel()
    record.value = 'Original Value'

    mod1 = DummyFieldModifier(identifier='xy0001')
    mod2 = DummyFieldModifier2(identifier='xy0002',
                               operands={"operand_1": "identifier.xy0001"})

    mod1.eval(record, chain)
    mod2.eval(record, chain)
    self.assertEquals('OKI', chain['xy0002'])

  def test_empty_method_bypasses_value(self):
    """ FieldModifier Empty method allows property rename """

    chain = {}
    record = DummyModel()
    record.prop_a = 'ABCDE 1'

    definition = {
      "identifier": "xy0002",
      "operands": {
        "value": "model.prop_a"
      }
    }

    mod = FieldModifier.from_dict(definition)
    mod.eval(record, chain)
    self.assertEqual('ABCDE 1', chain['xy0002'])


if __name__ == '__main__':

  suite = unittest.TestSuite()
  suite.addTest(unittest.makeSuite(TestFieldModifier))
