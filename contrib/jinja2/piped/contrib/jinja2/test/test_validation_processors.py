# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from piped.contrib.jinja2 import processors
from twisted.python import reflect
from twisted.trial import unittest


class StubCreator(processors.InstanceCreator):
    def process(self, baton):
        raise NotImplementedError()


class TestInstanceCreateor(unittest.TestCase):

    def _create_processor(self):
        return StubCreator()

    def test_processor_invokes_the_schema(self):
        processor = self._create_processor()
