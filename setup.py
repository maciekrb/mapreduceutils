#!/usr/bin/env python
from distutils.core import setup

setup(
  name="datastoreutils",
  version="0.1",
  author="Maciek Ruckgaber",
  author_email="maciekrb@gmail.com",
  description="Python module for transforming BigTable Datastore data",
  long_description=open('README.rst').read(),
  packages=["datastoreutils"],
  license="BSD License",
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Environment :: Google Appengine',
    'Intended Audience :: Appengine datastore users',
    'License :: BSD License',
    'Operating System :: OS Independent',
    'Topic :: Software Development :: Libraries'
  ]
)
