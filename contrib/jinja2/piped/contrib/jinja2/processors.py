# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import jinja2
from twisted.python import reflect
from zope import interface

from piped import util, processing, yamlutil
from piped.processors import base


class InstanceCreator(base.Processor):

    def _create_instance(self, instance_factory_name, instance_arguments, baton=None):
        instance_factory_name = self.get_input(baton, instance_factory_name)
        instance_kwargs = self._realize_instance_kwargs(instance_arguments, baton)

        return reflect.namedAny(instance_factory_name)(**instance_kwargs)

    def _realize_instance_kwargs(self, instance_arguments, baton=None):
        kwargs = dict()
        for key_path, value in util.dict_iterate_paths(instance_arguments):
            if isinstance(value, (list, tuple)):
                realized_values = list()
                for item in value:
                    realized_values.append(self.get_input(baton, item))
                value = realized_values

            value = self.get_input(baton, value)
            util.dict_set_path(kwargs, key_path, value)

        return kwargs


class CreateJinja2Loader(InstanceCreator):
    interface.classProvides(processing.IProcessor)
    name = 'create-jinja2-loader'

    def __init__(self, loader, loader_arguments=None, volatile=True, output_path='loader', **kw):
        super(CreateJinja2Loader, self).__init__(**kw)

        self.loader = None
        self.loader_name = loader
        self.loader_arguments = loader_arguments or dict()

        self.volatile = volatile
        self.output_path = output_path

    def configure(self, runtime_environment):
        if self.volatile:
            return

        self.loader = self._create_instance(self.loader_name, self.loader_arguments)

    def process(self, baton):
        loader = self.loader
        if not loader:
            loader = self._create_instance(self.loader_name, self.loader_arguments, baton)

        return self.get_resulting_baton(baton, self.output_path, loader)


class CreateJinja2Environment(InstanceCreator):
    interface.classProvides(processing.IProcessor)
    name = 'create-jinja2-environment'

    def __init__(self, environment, environment_arguments=None, volatile=True, output_path='environment', **kw):
        super(CreateJinja2Environment, self).__init__(**kw)

        self.environment = None
        self.environment_name = environment
        self.environment_arguments = environment_arguments or dict()

        self.volatile = volatile
        self.output_path = output_path

    def configure(self, runtime_environment):
        if self.volatile:
            return

        self.environment = self._create_instance(self.environment_name, self.environment_arguments)

    def process(self, baton):
        environment = self.environment
        if not environment:
            environment = self._create_instance(self.environment_name, self.environment_arguments, baton)

        return self.get_resulting_baton(baton, self.output_path, environment)


class LoadJinja2Template(InstanceCreator):
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

from twisted.internet import threads

class RenderJinja2Template(InstanceCreator):
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