import types
from google.appengine.ext.db import Key as dbkey
from google.appengine.ext.ndb import Key as ndbkey


__all__ = [
  "for_name",
  "handler_for_name",
  "parse_model_path"
]


def for_name(fq_name, recursive=False):
  """Find class/function/method specified by its fully qualified name.

  Fully qualified can be specified as:
    * <module_name>.<class_name>
    * <module_name>.<function_name>
    * <module_name>.<class_name>.<method_name> (an unbound method will be
      returned in this case).

  for_name works by doing __import__ for <module_name>, and looks for
  <class_name>/<function_name> in module's __dict__/attrs. If fully qualified
  name doesn't contain '.', the current module will be used.

  Args:
    fq_name: fully qualified name of something to find.
    recursive: run recursively or not.

  Returns:
    class object or None if fq_name is None.

  Raises:
    ImportError: when specified module could not be loaded or the class
    was not found in the module.
  """
#  if "." not in fq_name:
#    raise ImportError("'%s' is not a full-qualified name" % fq_name)

  if fq_name is None:
    return

  fq_name = str(fq_name)
  module_name = __name__
  short_name = fq_name

  if fq_name.rfind(".") >= 0:
    (module_name, short_name) = (fq_name[:fq_name.rfind(".")],
                                 fq_name[fq_name.rfind(".") + 1:])

  try:
    result = __import__(module_name, None, None, [short_name])
    return result.__dict__[short_name]
  except KeyError:
    # If we're recursively inside a for_name() chain, then we want to raise
    # this error as a key error so we can report the actual source of the
    # problem. If we're *not* recursively being called, that means the
    # module was found and the specific item could not be loaded, and thus
    # we want to raise an ImportError directly.
    if recursive:
      raise
    else:
      raise ImportError("Could not find '%s' on path '%s'" % (
          short_name, module_name))
  except ImportError:
    # module_name is not actually a module. Try for_name for it to figure
    # out what's this.
    try:
      module = for_name(module_name, recursive=True)
      if hasattr(module, short_name):
        return getattr(module, short_name)
      else:
        # The module was found, but the function component is missing.
        raise KeyError()
    except KeyError:
      raise ImportError("Could not find '%s' on path '%s'" % (
          short_name, module_name))
    except ImportError:
      # This means recursive import attempts failed, thus we will raise the
      # first ImportError we encountered, since it's likely the most accurate.
      pass
    # Raise the original import error that caused all of this, since it is
    # likely the real cause of the overall problem.
    raise


def handler_for_name(fq_name):
  """Resolves and instantiates handler by fully qualified name.

  First resolves the name using for_name call. Then if it resolves to a class,
  instantiates a class, if it resolves to a method - instantiates the class and
  binds method to the instance.

  Args:
    fq_name: fully qualified name of something to find.

  Returns:
    handler instance which is ready to be called.
  """
  resolved_name = for_name(fq_name)
  if isinstance(resolved_name, (type, types.ClassType)):
    # create new instance if this is type
    return resolved_name()
  elif isinstance(resolved_name, types.MethodType):
    # bind the method
    return getattr(resolved_name.im_class(), resolved_name.__name__)
  else:
    return resolved_name


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
