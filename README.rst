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
Through simple configuration params, allows to filter out, choose which properties to export and apply column transforms.

The utility assumes you would be using expando tables with non uniform "schemas", and so allows to export distinct model attributes based on simple rules.

Usage
=====
Basic usage as follows, record_map does the tricks here.

```python
  # Create a basic Pipeline for sending links with exported data

  from mapreduce import base_handler
  from mapreduce import input_readers, output_handlers
  from datastoreutils import record_map

  class ExportPipeline(base_handler.PipelineBase):
    def run(self, kind, property_map, to, num_shards=6):

      out_data = yield mapreduce_pipeline.MapperPipeline(
        "Datastore exporting Pipeline",
        __name__ + ".record_map",
        input_reader_spec=input_readers.__name__ + ".DatastoreInputReader",
        output_writer_spec=output_writers.__name__ + ".BlobstoreOutputWriter",
        params={
          "input_reader": {
            "entity_kind": kind
          },
          "output_writer": {
            "mime_type": "text/csv"
          },
          property_map: property_map
        },
        shards=num_shards
      )

      yield SendDownloadLink(out_data, mail=to, subject="Your export succeeded!")


  # Now let's just export 3 different attributes from records with different structure, that
  # make sense combined
  export_pipeline = ExportPipeline(
    kind="app.models.SomeModel",  # This can be an expando
    property_map=[{  # This is the secret sauce !
      "property_match_name": "book_gender", # this will try to retrieve SomeModelInstance.book_gender
      "property_match_value": "sci-fi", # this will try to match "sci-fi" as the value of .book_gender
      "property_list": [  # Only exported when the above are a match
        "date_published",
        "scifi_score",
        "some_other_scifi_attr"
      ]
    },{
      "property_match_name": "book_gender",
      "property_match_value": "calculus",
      "property_list": [
        "date_published",
        "math_score",
        "some_other_math_equiv_attr"
      ]
    }],
    to="someguy@example.com"
  )
  export_pipeline.start()

  # More doc coming soon !

```
