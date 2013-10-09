import unittest
from google.appengine.ext import db
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from datastoreutils import KeyModelMatchRule, ModelRuleSet, PropertyMap

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

  def test_key_model_match_rule_class_raises_error(self):
    """ PropertyModelMatchRule raises proper error on bad argument """

    with self.assertRaises(ValueError) as ctx:
      key_rule = KeyModelMatchRule('XXX')

    self.assertEqual("Path should be a list of tuples (Model, id/name): XXX", ctx.exception.message)



class TestModelRuleSet(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def test_model_rule_set_key_rule_creation(self):
    """ ModelRuleSet instance creates a valid key from arguments"""
    rset = ModelRuleSet()

    # Model key Rule
    path = db.Key.from_path('ABC', 1, 'BCD', 2)
    rset.set_key_rule(path)

    self.assertIsInstance(rset.get_key_rule(), KeyModelMatchRule)
    self.assertEqual((('ABC', 1), ('BCD', 2)), rset.get_key_rule().rule())

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

    # Property filters
    rset.add_property_filter("property_something","=", 3)
    rset.add_property_filter("property_something_else","IN", ["a","b"])

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
      ],
      "property_filters": [
        ("property_something", "=", 3),
        ("property_something_else", "IN", ["a","b"])
      ]
    }
    self.assertEqual(expected, rset.to_dict())

class TestPropertyMap(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def test_property_map_consistency(self):
    """ ModelRuleSet instances are serialized correctly """

    pmap = PropertyMap()

    # First RuleSet
    rset1 = ModelRuleSet()
    path = db.Key.from_path('ABC', 1, 'BCD', 2)
    rset1.set_key_rule(path)
    rset1.add_model_property("property_x")
    rset1.add_model_property("property_y")

    # Second RuleSet

    # Property filters
    rset2 = ModelRuleSet()
    path = db.Key.from_path('ABC', 1, 'BCD', 30)
    rset2.set_key_rule(path)
    rset2.add_property_filter("property_something","=", 3)
    rset2.add_property_filter("property_something_else","IN", ["a","b"])
    rset2.add_model_property("property_m")
    rset2.add_model_property("property_a")

    pmap.add_model_ruleset('set1', rset1)
    pmap.add_model_ruleset('set2', rset2)

    # Getter works fine
    self.assertEqual(rset1, pmap.get_model_ruleset('set1'))

    expected = [{
      "model_match_rule": {
        "key": (('ABC', 1), ('BCD', 2))
      },
      "property_list": [
        "property_x",
        "property_y"
      ]
    },
    {
      "model_match_rule": {
        "key": (('ABC', 1), ('BCD', 30))
      },
      "property_list": [
        "property_m",
        "property_a"
      ],
      "property_filters": [
        ("property_something", "=", 3),
        ("property_something_else", "IN", ["a","b"])
      ]
    }]
    self.assertEqual(expected, pmap.to_dict())

