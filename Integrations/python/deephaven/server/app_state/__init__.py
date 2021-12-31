from deephaven.server.script_session import unwrap_to_java_type

class ApplicationState:
    def __init__(self, state):
        self.state = state

    def set_field(self, name, value):
        # the workaround for setting params
        java_obj = unwrap_to_java_type(value)
        if java_obj:
            self.state.set_field_java(name, java_obj)
        else:
            self.state.set_field_python(name, value)

class ApplicationAdapter:
    def __init__(self, application_module):
        self.application_module = application_module

    def id(self):
        return self.application_module.id()

    def name(self):
        return self.application_module.name()

    def initializeInto(self, jstate):
        # todo: https://github.com/deephaven/deephaven-core/issues/1775
        import jpy
        Forker = jpy.get_type('io.deephaven.server.plugin.Forker')
        return self.application_module.initialize_into(StateAdapter(Forker(jstate)))

class StateAdapter:
    def __init__(self, jstate_fork):
        self.jstate_fork = jstate_fork

    def set_field(self, name, value):
        # the workaround for setting params
        java_obj = unwrap_to_java_type(value)
        if java_obj:
            self.jstate_fork.setFieldJava(name, java_obj)
        else:
            self.jstate_fork.setFieldPython(name, value)
