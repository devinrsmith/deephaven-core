package io.deephaven.engine.util;

import org.jpy.PyLib.CallableKind;
import org.jpy.PyModule;
import org.jpy.PyObject;

import java.io.Closeable;

// private
public interface PythonScriptSessionModule extends Closeable {
    static PythonScriptSessionModule of() {
        return (PythonScriptSessionModule) PyModule.importModule("deephaven.server.script_session")
                .createProxy(CallableKind.FUNCTION, PythonScriptSessionModule.class);
    }

    PyObject create_change_list(PyObject from, PyObject to);

    // TODO(deephaven-core#1775): multivariate jpy (unwrapped) type conversion into java\
    Object unwrap_to_java_type(PyObject object);

    void close();
}
