/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.python;

import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.StreamExporterImpl;
import org.jpy.PyObject;

import java.nio.ByteBuffer;
import java.util.Objects;

final class ObjectTypeAdapter extends ObjectTypeBase implements AutoCloseable {

    private final String name;
    private final PyObject objectTypeAdapter;

    public ObjectTypeAdapter(String name, PyObject objectTypeAdapter) {
        this.name = Objects.requireNonNull(name);
        this.objectTypeAdapter = Objects.requireNonNull(objectTypeAdapter);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isType(Object object) {
        if (!(object instanceof PyObject)) {
            return false;
        }
        return objectTypeAdapter.call(boolean.class, "is_type", PyObject.class, (PyObject) object);
    }

    @Override
    public MessageStream compatibleClientConnection(Object object, MessageStream connection) {
        if (objectTypeAdapter.hasAttribute("create_client_connection")) {
            PyObject newConnection =
                    objectTypeAdapter.call(PyObject.class, "create_client_connection", PyObject.class,
                            (PyObject) object, MessageStream.class, connection);
            return new PythonMessageStream(newConnection);
        } else {
            // Fall back and attempt to use old api:
            // Using this simple implementation, even though the python code won't write to this, but instead will
            // return a byte array directly
            StreamExporterImpl exporter = new StreamExporterImpl();

            final byte[] bytes = objectTypeAdapter.call(byte[].class, "to_bytes",
                    ExporterAdapter.class, new ExporterAdapter(exporter),
                    PyObject.class, (PyObject) object);

            // Send the message and close the stream
            connection.onData(ByteBuffer.wrap(bytes), exporter.references());
            connection.onClose();

            return MessageStream.NOOP;
        }
    }

    private static class PythonMessageStream implements MessageStream {
        private final PyObject instance;

        private PythonMessageStream(PyObject instance) {
            this.instance = instance;
        }

        @Override
        public void onData(ByteBuffer payload, Object[] references) {
            instance.call(void.class, "on_message", byte[].class, payload.array(), Object[].class, references);
        }

        @Override
        public void onClose() {
            instance.call("close");
        }
    }

    @Override
    public String toString() {
        return objectTypeAdapter.toString();
    }

    @Override
    public void close() {
        objectTypeAdapter.close();
    }
}
