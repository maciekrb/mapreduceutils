
import unittest
from google.appengine.ext import db
from google.appengine.ext import testbed
from datastoreutils import _get_key_pairs, _validates_key_filters, _record_matches_filters

class SampleModel(db.Model):
  pass

class TestRecordFilters(unittest.TestCase):

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()

  def tearDown(self):
    self.testbed.deactivate()

  def test_key_pair_resolver(self):
    """ Key pairs are correctly resolved from from a db Key object """

    key = db.Key.from_path('ABC', 1)
    self.assertEqual([('ABC', 1)], _get_key_pairs(key))

    key = db.Key.from_path('ABC', 1, 'BCD', 2)
    self.assertEqual([('ABC', 1), ('BCD', 2)], _get_key_pairs(key))

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'CDE', 3)
    self.assertEqual([('ABC', 1), ('BCD', 2), ('CDE', 3)], _get_key_pairs(key))

    key = db.Key.from_path('ABC', 1, 'BCD', "aaaa", 'CDE', 3)
    self.assertEqual([('ABC', 1), ('BCD', "aaaa"), ('CDE', 3)], _get_key_pairs(key))


  def test_key_filters(self):
    """ Record key filtering in record matching works fine """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleModel', 3)
    model = SampleModel(key=key)

    pairs = _get_key_pairs(model.key())
    res = _validates_key_filters(pairs, None)
    self.assertEqual(True, res)

    pairs = _get_key_pairs(model.key())
    res = _validates_key_filters(pairs, [(('ABC', 1),)])
    self.assertEqual(True, res)

    pairs = _get_key_pairs(model.key())
    res = _validates_key_filters(pairs, [(('ABC', 1), ('BCD', 2))])
    self.assertEqual(True, res)

    pairs = _get_key_pairs(model.key())
    res = _validates_key_filters(pairs, [(('ABC', 2), ('BCD', 2))])
    self.assertEqual(False, res)

    pairs = _get_key_pairs(model.key())
    res = _validates_key_filters(pairs, [(('ABC', 1), ('CDE', 2))])
    self.assertEqual(False, res)

    pairs = _get_key_pairs(model.key())
    res = _validates_key_filters(pairs, [(('ABC', 1), ('CDE', 2)), (('ABC', 1), ('BCD', 2))])
    self.assertEqual(True, res)

    pairs = _get_key_pairs(model.key())
    res = _validates_key_filters(pairs, [(('ABC', 1), ('BCD', 2)), (('ABC', 1), ('CDE', 2))])
    self.assertEqual(True, res)

  def test_record_filter_match(self):
    """ Combinations of key_filters and property filters match """

    key = db.Key.from_path('ABC', 1, 'BCD', 2, 'SampleModel', 3)
    model = SampleModel(key=key)
    model.a = 'test'
    model.b = 123

    """ key only filter success """
    pfilters = {}
    kfilters = [(('ABC', 1), ('BCD', 2))]
    res = _record_matches_filters(model, pfilters, kfilters)
    self.assertEqual(True, res)

    """ key only filter fail """
    pfilters = {}
    kfilters = [(('ABC', 1), ('BCE', 2))]
    res = _record_matches_filters(model, pfilters, kfilters)
    self.assertEqual(False, res)

    """ key filter with property success """
    pfilters = [('a', '=', 'test')]
    kfilters = [(('ABC', 1), ('BCD', 2))]
    res = _record_matches_filters(model, pfilters, kfilters)
    self.assertEqual(True, res)

    """ key filter with property fail """
    pfilters = [('a', '=', 'badtest')]
    kfilters = [(('ABC', 1), ('BCD', 2))]
    res = _record_matches_filters(model, pfilters, kfilters)
    self.assertEqual(False, res)

    """ key filter failure with good property filter """
    pfilters = [('a', '=', 'test')]
    kfilters = [(('ABC', 1), ('BCD', 3))]
    res = _record_matches_filters(model, pfilters, kfilters)
    self.assertEqual(False, res)



