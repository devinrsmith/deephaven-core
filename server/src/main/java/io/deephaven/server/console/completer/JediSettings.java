package io.deephaven.server.console.completer;

import org.jpy.PyObject;

import java.io.Closeable;

public interface JediSettings extends Closeable {

    void open_doc(String text, String uri, int version);

    String get_doc(String uri);

    void update_doc(String document, String uri, int version);

    void close_doc(String uri);

    boolean is_enabled();

    PyObject do_completion(PyObject scope, String uri, int version, int line, int character);

    boolean can_jedi();

    @Override
    void close();
}
