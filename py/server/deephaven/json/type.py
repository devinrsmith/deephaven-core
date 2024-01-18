import jpy
from deephaven._wrapper import JObjectWrapper

_JValueOptions = jpy.get_type("io.deephaven.json.ValueOptions")


class JsonOptions(JObjectWrapper):
    j_object_type = _JValueOptions

    def __init__(self, j_options: jpy.JType):
        self.j_options = j_options

    @property
    def j_object(self) -> jpy.JType:
        return self.j_options
