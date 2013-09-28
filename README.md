About
=====

Simple Library for performing Appengine Datastore transformation operations

Requirements
============
You will need the mapreduce code from: https://code.google.com/p/appengine-mapreduce
You can also check it out through Subversion here:
`svn checkout http://appengine-mapreduce.googlecode.com/svn/trunk/ appengine-mapreduce-read-only`

Functionality
=============
Through simple rules, allows to filter out or modify, App Engine Datastore models via Mapreduce, 
even when using expando models with very different properties. Works with older db models and the
newer ndb models.

Say you have a model like :
```python
class TestClass(db.Expando):
  attr1 = db.StringProperty()
  attr2 = db.DateTimeProperty()

# We now insert this in the database 
obj = TestClass(attr1="One", attr2=some_datetime, expando1="Something Else")
obj.put()

obj = TestClass(attr1="Two", attr2=some_datetime, expando9="Something Else")
obj.put()

obj = TestClass(attr1="Three", attr2=some_datetime, expando30="Something Else", more_expando_fun="Yup!")
obj.put()

# ... Thousands of times ... 
```

You then decide to export the data into a single file, but then, since you have different "schemas" 
of your objects, how do you actually make some structure out of this ?

I'll leave you the fun of deciding which expando attributes make sense to be in the same "column", 
but this utility will make it easy to map those attributes to "columns" once you've got your schema.

In the end you should be able to get something like this:

| col a | col b | col c |
| ----  | ----  | ----  |
| One   | 2010-03-10 | Something Else |
| Two   | 2010-03-10 | Something Else |
| Three   | 2010-03-10 | Yup! |

Or this, if that is works better for you, with just a tiny config change

| col a | col b | col c |
| ----  | ----  | ----  |
| One   | 2010-03-10 | Something Else |
| Two   | 2010-03-10 | Something Else |
| Three   | 2010-03-10 | Something Else |


Usage
=====
The result can be obtained by executing a Mapper Pipeline using the 
`datastoreutils.record_map`  mapper and providing a bit of configuration:

The configuration would look something like this : 
```python
property_map = [
  {
    "model_match_rule": {
      "properties" : [("attr1", "One")],
    },
    "property_list": [
      "attr1",
      "attr2",
      "expando1",
    ]
  },
  {
    "model_match_rule": {
      "properties" : [("att1", "Two")],
    },
    "property_list": [
      "attr1",
      "attr2",
      "expando9"
    ]
  },
  {
    "model_match_rule": {
      "properties" : [("att1", "Three")],
    },
    "property_list": [
      "attr1",
      "attr2",
      "more_expando_fun"
    ]
  }
]
```

The property map is a list of "rules"  that will attempt to match records being read from 
`DatastoreIntputReader` as they reach de `datastoreutils.record_map` function. If the record
matches, it is yielded as a comma separated value, otherwise it is ignored.

In the above example every "rule" will match one specific case from the models inserted at
the beginning. The first one will try to match an attribute named `attr1` with a value of `One`, 
and in case there is a match, will yield attributes `attr1`, `attr2` and `expando1`. In the 
next case, any record with an `attr1` with value `Two` will yield attributes `attr1`, `attr2` and
`expando9` and so on. 

the `model_match_rule` can either match `properties` given as tuples `(key, value)` or model
Keys `db.Key` or `ndb.Key`, given as a path: 
```python
  model_match_rule = {
    "key" : (('ModelA', 3445), ('ModelB', 4322))
  }
```

In which case keys will be validated from left to right on top of the record key. The above
example will match any `Key` who's path starts with `('ModelA', 3443)` and continues with
`('ModelB', 4332)` regardles of the key pairs that follow. You can use keys containing either ids 
or names safely.

More complex matching rules involving keys and properties are also allowed:
```python
  model_match_rule = {
    "key" : (('ModelA', 3445), ('ModelB', 4322)),
    "properties": [("attr1", "Three"), ("attr2", "something")]
  }
```

Be careful though that the rules are matched in order as the record reaches the 
`datastoreutils.record_map` function, and thus the first matched rule is applied, meaning that you
should put your more specific rules before the general ones.


The full pipleline would look like this :

```python
  # Create a basic Pipeline for sending links with exported data

  from mapreduce import base_handler
  from mapreduce import input_readers, output_handlers
  from datastoreutils import record_map

  class ExportPipeline(base_handler.PipelineBase):
    def run(self, kind, property_map, num_shards=6):

      out_data = yield mapreduce_pipeline.MapperPipeline(
        "Datastore exporting Pipeline",
        __name__ + ".record_map", # datastoreutils map function
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=output_writers.__name__ + ".BlobstoreOutputWriter",
        params={
          "input_reader": {
            "entity_kind": kind
          },
          "output_writer": {
            "mime_type": "text/csv"
          },
          property_map: property_map # datastoreutils map function model resolution rules
        },
        shards=num_shards
      )

      yield SomeOtherThingWithYourData(out_data)
```

You run this as follows : 
```python

  export_pipeline = ExportPipeline(
    kind="app.models.SomeModel",  # This can be an expando Model
    property_map=property_map # Your secret sauce mapping
  )
  export_pipeline.start()

```

Modifiers
=========

The library implements a simple modifier API, so instead of yielding just attributes, you might
generate a modified value, say a different date format based on a DateTime attribute from the 
model. This could be accomplished in the following way:

```python
property_map = [
  {
    "model_match_rule": {
      "properties" : [("attr1", "One")],
    },
    "property_list": [
      "attr1",
      [{
        "method": "datastoreutils.DateModifier",
        "identifier": "some_name_for_chaining",
        "operands": {"date" : "model.attr2"},
        "args": {"date_format": "%m"}
      }],
      "expando1",
    ]
  },
  {
    "model_match_rule": {
      "properties" : [("att1", "Two")],
    },
    "property_list": [
      "attr1",
      [{
        "method": "datastoreutils.DateModifier",
        "identifier": "some_name_for_chaining",
        "operands": {"date" : "model.attr2"},
        "args": {"date_format": "%m"}
      }],
      "expando9"
    ]
  },
  {
    "model_match_rule": {
      "properties" : [("att1", "Three")],
    },
    "property_list": [
      "attr1",
      [{
        "method": "datastoreutils.DateModifier",
        "identifier": "some_name_for_chaining",
        "operands": {"date" : "model.attr2"},
        "args": {"date_format": "%m"}
      }],
      "more_expando_fun"
    ]
  }
]
```

So as you see, we just replaced the nice little string containing the attribute name by a chunk of code
with some odd names. The odd structure allows to evaluate `FieldModifier`  classes. In the above example, instead of getting the full date representation, we would get only the `%m` part of the date.

### Method
Defines the class that will handle the transformation as a string. 

### Identifier
It is an arbitrary name given to the result of the transformation so it can be chained.

### Operands
Provides operands to the transformation class, which are normally model attributes or identifiers of
previous transformations. Model attributes must be prepended with `model.name_of_attribute` and 
identifiers of previous transformations must be prepended with `identifier.name_of_transformation`.

### Args
Additional arguments that may change the behavior of the transformation.

The transformations can be chained, by adding more than one modifier, keep in mind that these should
be light transformations, if you find yourself adding too many transformations, you might be better
creating a more complex pipeline instead.

This illustrates how to chain modifiers:
```python

  {
    "model_match_rule": {
      "properties" : [("att1", "Three")],
    },
    "property_list": [
      "attr1",
      [{
        "method": "datastoreutils.TimeZoneModifier",
        "identifier": "name_op_1",
        "operands": {"date" : "model.attr2"},
        "args": {"timezone": "Africa/Maseru"}
      },
      {
        "method": "datastoreutils.DateModifier",
        "identifier": "name_op_2",
        "operands": {"date" : "identifier.name_op_1"},
        "args": {"date_format": "%m"}
      }],
      "more_expando_fun"
    ]
  }

```

## TODO

- Model match rules that test for presence of an attribute without matching the value might be useful.
- More General use Modifier functions would be really nice.
