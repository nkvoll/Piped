# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from piped.contrib.jinja2 import processors
from twisted.trial import unittest


class TestLoadJinja2Template(unittest.TestCase):

    def _create_processor(self, *a, **kw):
        return processors.LoadJinja2Template(*a, **kw)


class TestRenderJinja2Template(unittest.TestCase):

    def _create_processor(self, *a, **kw):
        return processors.RenderJinja2Template(*a, **kw)
