import logging
import datetime
import jinja2
from app.config import notification_templates_path
from mapreduce import base_handler
from mapreduce.input_readers import RecordsReader
from mapreduce.lib.files import file_service_pb
from mapreduce.util import handler_for_name
from google.appengine.ext import db
from app.models import ReducedRecord, IndicatorEntry, Notification, SystemUser
from webapp2_extras import json
from webapp2_extras.i18n import gettext as _


def render_template(template_name, template_vals=None):
  env = jinja2.Environment(loader=jinja2.FileSystemLoader(notification_templates_path))
  return env.get_template("%s.html" % template_name).render(template_vals or {})


def unique(seq):
  """ Forma eficiente de dejar elementos unicos en una lista """
  seen = set()
  seen_add = seen.add
  return [x for x in seq if x not in seen and not seen_add(x)]


def bypass(values):
  """
  Returns Key Value Pairs untouched
  """
  for k, v in values:
    yield k, v


def percent(values):
  """
  Calculates percentage of each value
  """
  total = sum([float(v) for k, v in values])
  if total > 0.0:
    for k, v in values:
      val = float(v)
      if val > 0:
        p = val * 100 / total
        yield k, p


def store_reduced_record(groupings, value, params):
  data = {
    'batch_id': params['batch_id'],
    'indicator_entry': db.Key(params['indicator_entry']),
    'indicator_path': db.Key(params['indicator_path']),
    'context': db.Key(params['context']),
    'value': value
  }

  for idx, p in enumerate(groupings, start=1):
    data["level%s" % idx] = p

  record = ReducedRecord(**data)
  record.put()
  #yield op.db.Put(record) does not insert


class PostProcess(base_handler.PipelineBase):
  def run(self, filenames, postproc_funcs, reduced_record_ctx):
    self.key_concat_seq = reduced_record_ctx['key_concat_seq']

    reader = RecordsReader(filenames, 0)

    """ Run all the postproc funcs using results as args """
    for group_key, records in self.group_records(reader).iteritems():
      grp_part = [] if group_key == '__stub__' else group_key.split(self.key_concat_seq)

      for fname in postproc_funcs:
        func = handler_for_name(fname)
        processed = []
        for t in func(records):
          processed.append(t)
        records = processed

      for k, v in records:
        groupings = list(grp_part)
        if k:
          groupings.append(k)
        logging.warn("set for storage (%s,%s)" % (groupings, v))
        store_reduced_record(groupings, v, reduced_record_ctx)

    # Actualiza las propiedades de control del indicator_entry
    indicator_entry = IndicatorEntry.get(reduced_record_ctx['indicator_entry'])
    indicator_entry.status = u'calculated'
    indicator_entry.num_reduced_records = indicator_entry.number_reduced_records()
    indicator_entry.calculated_date = datetime.datetime.now()
    indicator_entry.put()

    # Crea la notificacion para el usuario
    user = SystemUser.get(reduced_record_ctx['user_key'])
    if user:
      notification = Notification(
        user=user.key(),
        type='miscelaneous',
        title=_('INDICATOR_ENTRY_CALCULATED'),
        message=render_template('indicator_entry_completed', {
          'indicator_entry_key': str(indicator_entry.key()),
          'indicator_entry_name': indicator_entry.name,
          'num_reduced_records': indicator_entry.num_reduced_records
        })
      )
      notification.put()

    # Obtiene todas las opciones de los niveles y las guarda en el indicator_entry
    indicator_path = indicator_entry.indicator_path

    levels = {}

    number_levels = len(indicator_path.grouping_mappings)
    for i in range(1, number_levels + 1):
      levels['level%s' % i] = []

    limit = 100
    offset = 0
    while True:
      reduced_records = ReducedRecord.all().filter('indicator_path =', indicator_path.key()).fetch(limit=limit,
                                                                                                   offset=offset)
      if len(reduced_records) == 0:
        break

      for reduced_record in reduced_records:
        for i in range(1, number_levels + 1):
          levels['level%s' % i].append(getattr(reduced_record, 'level%s' % i))

      offset += 100

    new_levels = {}
    for key, value in levels.iteritems():
      value = unique(value)
      value.sort()
      new_levels[key] = value
    del levels

    indicator_entry.level_options = json.encode(new_levels)
    indicator_entry.put()

  def group_records(self, reader):
    groups = {}
    for binary_record in reader:
      proto = file_service_pb.KeyValue()
      proto.ParseFromString(binary_record)
      key, val = self.explode_key(proto.key()), proto.value().decode('utf-8')
      if len(key) == 1:
        if '__stub__' not in groups:
          groups['__stub__'] = []
        groups['__stub__'].append((key, val))
      else:
        gk = self.key_concat_seq.join(key[:-1])
        if gk not in groups:
          groups[gk] = []
        groups[gk].append((key[-1], val))

    return groups

    """ Pop the low level key and group with the rest  """

  def explode_key(self, key):
    key = key.decode('utf-8')
    parts = key.split(self.key_concat_seq)
    return parts
