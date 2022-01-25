package io.deephaven.server.plugin;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.Registration.Callback;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.server.plugin.python.PythonPluginRegistration;

import javax.inject.Inject;
import java.util.Objects;
import java.util.Set;

/**
 * Provides a {@link #registerAll()} entrypoint for {@link Registration} auto-discovery. Logs auto-discovered details.
 */
public final class PluginsAutoDiscovery {
    private static final Logger log = LoggerFactory.getLogger(PluginsAutoDiscovery.class);

    private final Set<Registration> registrations;
    private final Registration.Callback callback;

    @Inject
    public PluginsAutoDiscovery(Set<Registration> registrations, Registration.Callback callback) {
        this.registrations = Objects.requireNonNull(registrations);
        this.callback = Objects.requireNonNull(callback);
    }

    /**
     * Registers {@link Registration plugins} via {@link JavaServiceLoader#loadAllAndRegisterInto(Callback)} and
     * {@link PythonPluginRegistration#allRegisterInto(Callback)} (if python is enabled).
     */
    public void registerAll() {
        log.info().append("Registering plugins...").endl();
        final Counting counting = new Counting();
        for (Registration registration : registrations) {
            registration.registerInto(counting);
        }
        log.info().append("Registered plugins: ").append(counting).endl();
    }

    private class Counting implements Registration.Callback, LogOutputAppendable, Plugin.Visitor<Counting> {

        private int objectTypeCount = 0;

        @Override
        public void register(Plugin plugin) {
            plugin.walk(this);
        }

        @Override
        public Counting visit(ObjectType objectType) {
            log.info().append("Registering object type: ")
                    .append(objectType.name()).append(" / ")
                    .append(objectType.toString())
                    .endl();
            callback.register(objectType);
            ++objectTypeCount;
            return this;
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("objectType=").append(objectTypeCount);
        }
    }
}
