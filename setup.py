#!/usr/bin/env python
from setuptools import setup

setup(
  name="mapreduceutils",
  version="2.3",
  author="Maciek Ruckgaber",
  author_email="maciekrb@gmail.com",
  description="Python mapreduce utilities for Google App Engine",
  long_description=open('README.md').read(),
  packages=["mapreduceutils", "mapreduceutils.modifiers"],
  license="BSD License",
  classifiers=[
    'Development Status :: 2',
    'Environment :: Google Appengine',
    'Intended Audience :: Appengine Mapreduce users',
    'License :: BSD License',
    'Operating System :: OS Independent',
    'Topic :: Software Development :: Libraries'
  ],
  test_suite='nose.collector',
)
