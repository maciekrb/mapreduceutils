import unittest
from google.appengine.ext import db
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from datastoreutils import KeyModelMatchRule, ModelRuleSet

class TestModelMatchRule(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def test_key_model_match_rule_class(self):
    """ KeyModelMatchRule instances correct rules from different arg types """

    # db.Key args
    path = db.Key.from_path('ABC', 1, 'BCD', 2)
    key_rule = KeyModelMatchRule(path)
    self.assertEqual((('ABC', 1), ('BCD', 2)), key_rule.rule())

    # ndb.Key args
    path = ndb.Key('ABC', 1, 'BCD', 2)
    key_rule = KeyModelMatchRule(path)
    self.assertEqual((('ABC', 1), ('BCD', 2)), key_rule.rule())

    # list args
    path = [('ABC', 1), ('BCD', 2)]
    key_rule = KeyModelMatchRule(path)
    self.assertEqual((('ABC', 1), ('BCD', 2)), key_rule.rule())

    # tuple args
    path = (('ABC', 1), ('BCD', 2))
    key_rule = KeyModelMatchRule(path)
    self.assertEqual((('ABC', 1), ('BCD', 2)), key_rule.rule())

  @unittest.skip("Unimplemented Test")
  def test_property_model_match_rule_class(self):
    """ PropertyModelMatchRule instances correct rules from different arg types """


class TestModelRuleSet(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def test_model_rule_set_consistency(self):
    """ ModelRuleSet instances are serialized correctly """

    rset = ModelRuleSet()

    # Model key Rule
    path = db.Key.from_path('ABC', 1, 'BCD', 2)
    rset.set_key_rule(path)

    # Property list and modifiers
    rset.add_model_property("property_x")
    rset.add_model_property("property_y")
    rset.add_modifier_property(
      group="property_m",
      method="datastoreutils.modifiers.primitives.DateFormatModifier",
      identifier="xyza1",
      operands={"value": "model.some_date"},
      args={"date_format": "%W"}
    )
    rset.add_modifier_property(
      group="property_m",
      method="datastoreutils.modifiers.primitives.BlahModifier",
      identifier="xyza2",
      operands={"value": "model.some_date"},
      args={"date_format": "%W"}
    )
    rset.add_model_property("property_z")
    rset.add_modifier_property(
      group="property_bb",
      method="datastoreutils.modifiers.primitives.BlahModifier",
      identifier="xyza9",
      operands={"value": "model.some_date"},
      args={"date_format": "%W"}
    )

    expected = {
      "model_match_rule": {
        "key": (('ABC', 1), ('BCD', 2))
      },
      "property_list": [
        "property_x",
        "property_y",
        [{
          "method": "datastoreutils.modifiers.primitives.DateFormatModifier",
          "identifier": "xyza1",
          "operands": {"value": "model.some_date"},
          "args": {"date_format": "%W"}
        },
        {
          "method": "datastoreutils.modifiers.primitives.BlahModifier",
          "identifier": "xyza2",
          "operands": {"value": "model.some_date"},
          "args": {"date_format": "%W"}
        }],
        "property_z",
        [{
          "method": "datastoreutils.modifiers.primitives.BlahModifier",
          "identifier": "xyza9",
          "operands": {"value": "model.some_date"},
          "args": {"date_format": "%W"}
        }]
      ]
    }
    self.assertEqual(expected, rset.to_dict())

