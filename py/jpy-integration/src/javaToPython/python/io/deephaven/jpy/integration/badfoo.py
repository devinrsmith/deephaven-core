import io
import jpy

_j_out = jpy.get_type('java.lang.System').out

class BadFoo(io.TextIOBase):
    def __init__(self):
        raise Exception("I hate fun")

    def close(self):
        _j_out.println("time to close")
