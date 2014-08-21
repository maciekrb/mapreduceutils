#!/usr/bin/env python
"""
Modulo define funciones para reducir colecciones de valores a un unico valor
"""


def average_values(values):
  _floats = [float(x) for x in values]
  return sum(_floats, 0.0) / len(_floats)


def sum_values(values):
  _floats = [float(x) for x in values]
  return sum(_floats, 0.0);
