package io.deephaven.server.plugin.python;

import org.jpy.PyLib.CallableKind;
import org.jpy.PyModule;

interface Deephaven2ServerPlugin extends AutoCloseable {

    String MODULE = "deephaven2.server.plugin";

    static Deephaven2ServerPlugin of() {
        final PyModule module = PyModule.importModule(MODULE);
        if (module == null) {
            throw new IllegalStateException(String.format("Unable to find `%s` module", MODULE));
        }
        return (Deephaven2ServerPlugin) module.createProxy(CallableKind.FUNCTION, Deephaven2ServerPlugin.class);
    }

    void initialize_all_and_register_into(CallbackAdapter callback);

    @Override
    void close();
}
