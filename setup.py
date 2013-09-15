#!/usr/bin/env python
from setuptools import setup

setup(
  name="datastoreutils",
  version="1.0",
  author="Maciek Ruckgaber",
  author_email="maciekrb@gmail.com",
  description="Python mapreduce utilities for exporting / transforming Datastore entries",
  long_description=open('README.md').read(),
  packages=["datastoreutils"],
  license="BSD License",
  classifiers=[
    'Development Status :: 1 - Beta',
    'Environment :: Google Appengine',
    'Intended Audience :: Appengine datastore users',
    'License :: BSD License',
    'Operating System :: OS Independent',
    'Topic :: Software Development :: Libraries'
  ],
  test_suite='datastoreutils.tests.suite',
)
