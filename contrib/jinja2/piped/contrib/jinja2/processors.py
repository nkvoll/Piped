# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from zope import interface

from piped import util, processing
from piped.processors import base


class LoadJinja2Template(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'load-jinja2-template'

    def __init__(self, template, environment='environment', parent=None, globals=None, output_path='template', **kw):
        super(LoadJinja2Template, self).__init__(**kw)

        self.environment_path = environment

        self.template_name = template
        self.parent = parent
        self.globals = globals

        self.output_path = output_path

    def process(self, baton):
        environment = util.dict_get_path(baton, self.environment_path)

        template_name = self.get_input(baton, self.template_name)
        parent = self.get_input(baton, self.parent)
        globals = self.get_input(baton, self.globals)

        template = environment.get_or_select_template(template_name, parent, globals)

        return self.get_resulting_baton(baton, self.output_path, template)


class RenderJinja2Template(base.Processor):
    interface.classProvides(processing.IProcessor)
    name = 'render-jinja2-template'

    def __init__(self, template='template', context=None, output_path='rendered', **kw):
        super(RenderJinja2Template, self).__init__(**kw)

        self.template = template
        self.context = context
        if self.context is None:
            self.context = dict()
        self.output_path = output_path

    def process(self, baton):
        template = util.dict_get_path(baton, self.template)
        context = self._get_context(baton)
        rendered = template.render(**context)
        return self.get_resulting_baton(baton, self.output_path, rendered)

    def _get_context(self, baton):
        if isinstance(self.context, dict):
            context = dict()
            for key, value in self.context.items():
                context[key] = self.get_input(baton, value)
            return context

        return self.get_input(baton, self.context)