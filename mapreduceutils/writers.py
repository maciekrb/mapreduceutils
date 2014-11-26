import csv
from cStringIO import StringIO
import datetime
from json import JSONEncoder
import math


class OutputWriter:
  @classmethod
  def get_writer(cls, out_format):
    cls_name = '{}Writer'.format(out_format.upper())
    return globals()[cls_name]


class MapperJSONEncoder(JSONEncoder):
  def default(self, obj):

    if hasattr(obj, '__json__'):
      return getattr(obj, '__json__')()

    if isinstance(obj, datetime.datetime):
      return obj.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(obj, datetime.date):
      return obj.strftime("%Y-%m-%d")

    return JSONEncoder.default(self, obj)


class CSVWriter(OutputWriter):
  @classmethod
  def encode(cls, values):
    out_values = list()
    for val in values:
      if val is None:
        out_values.append(u'')
      else:
        out_values.append(unicode(val).encode('utf-8'))

    return out_values

  @classmethod
  def write(cls, data_obj):
    """
    Converts given fields to correctly formated CSV record

    Uses Python csv library to create proper CSV record from given list
    Args:
      - data_obj: (iterable) OrderedDict containing the data that should be
        converted to CSV.

    Returns:
      String with correctly formated CSV record, including line terminating
      characters.

    @TODO: add a param to spec other dialects
    """

    data = StringIO()
    writer = csv.writer(data)
    writer.writerow(cls.encode(data_obj.values()))
    return data.getvalue()


class JSONWriter(OutputWriter):
  @classmethod
  def write(cls, data_obj, nan_to_null=False):
    """
    Encodes given python object to JSON

    Args:
      data_obj:      Python object to encode to JSON
      nan_to_null:   (Bool) If True NaN values will be converted to None
                     before encoding to JSON
    Returns:
      JSON string
    """
    if nan_to_null:
      data_obj = {k: None if isinstance(v, float) and math.isnan(v) else v
                  for k, v in data_obj.items()}
    return "{}\r\n".format(MapperJSONEncoder().encode(data_obj))
