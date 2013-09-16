import unittest
from datastoreutils.modifiers import FieldModifier

class DummyFieldModifier(FieldModifier):
  _META_NAME = "Test Name"
  _META_DESCRIPTION = "Test Description"
  _META_ARGS = {
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
  _META_OPERANDS = {
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
  _META_NAME = "Test Name"
  _META_DESCRIPTION = "Test Description"
  _META_ARGS = {
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
  _META_OPERANDS = {
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
    mod = DummyFieldModifier()
    self.assertEqual(mod.meta_name, "Test Name")
    self.assertEqual(mod.meta_description, "Test Description")
    self.assertIsInstance(mod.meta_arguments, dict)
    self.assertIsInstance(mod.meta_operands, dict)

  def test_instantiation_from_dict(self):
    definition = {
      "identifier": "xy0002",
      "method": "datastoreutils.tests.modifiers.test_fieldmodifier.DummyFieldModifier",
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

  def test_modifier_return_value(self):

    chain = {}
    record = DummyModel()
    modifier = DummyFieldModifier(identifier='xy0001')
    modifier.eval(record, chain)
    self.assertEqual('testvalue', chain['xy0001'])

  def test_modifier_chaining(self):

    chain = {}
    record = DummyModel()
    record.value = 'Original Value'

    mod1 = DummyFieldModifier(identifier='xy0001')
    mod2 = DummyFieldModifier2(identifier='xy0002', operands={"operand_1": "identifier.xy0001"})

    mod1.eval(record, chain)
    mod2.eval(record, chain)
    self.assertEquals('OKI', chain['xy0002'])

if __name__ == '__main__':

  suite = unittest.TestSuite()
  suite.addTest(unittest.makeSuite(TestFieldModifier))
