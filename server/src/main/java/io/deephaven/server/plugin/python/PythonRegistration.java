package io.deephaven.server.plugin.python;

import dagger.Binds;
import dagger.multibindings.IntoSet;
import io.deephaven.plugin.Registration;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;

import javax.inject.Inject;

/**
 * Registers all {@link Registration plugins} found via python method "deephaven.plugin:register_all_into". See the
 * deephaven-plugin python package for more information.
 */
public final class PythonRegistration implements Registration {

    @Inject
    public PythonRegistration() {
    }

    @Override
    public void registerInto(Callback callback) {
        if (!ConsoleServiceGrpcImpl.isPythonSession()) {
            return;
        }
        try (final Deephaven2ServerPluginModule module = Deephaven2ServerPluginModule.of()) {
            module.initialize_all_and_register_into(new CallbackAdapter(callback));
        }
    }

    @dagger.Module
    public interface Module {

        @Binds
        @IntoSet
        Registration bindsPythonRegistration(PythonRegistration registration);
    }
}
