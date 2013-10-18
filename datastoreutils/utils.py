from google.appengine.ext.db import Key as dbkey
from google.appengine.ext.ndb import Key as ndbkey

def parse_model_path(path):
  """ Parses different values of path into a list of tuples """

  if isinstance(path, ndbkey):
    rule = path.pairs()
  elif isinstance(path, dbkey):
    rule = ndbkey.from_old_key(path).pairs()
  elif all(isinstance(p, (tuple, list)) for p in path):
    rule = tuple(path)
  else:
    raise ValueError("Path should be a list of tuples (Model, id/name): {}".format(path))

  return rule
