package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationConfig;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.AppLookup;
import io.deephaven.plugin.app.ConsumerBase;
import io.deephaven.util.SafeCloseable;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Objects;

public class PluginApplicationInjector {

    private static final Logger log = LoggerFactory.getLogger(PluginApplicationInjector.class);

    private final Applications applications;
    private final ApplicationState.Listener applicationListener;
    private final AppLookup apps;

    @Inject
    public PluginApplicationInjector(final Applications applications,
            final ApplicationState.Listener applicationListener,
            final AppLookup apps) {
        this.applications = Objects.requireNonNull(applications);
        this.applicationListener = Objects.requireNonNull(applicationListener);
        this.apps = Objects.requireNonNull(apps);
    }

    public void run() throws IOException, ClassNotFoundException {
        // todo: should the directory configuration still apply to plugin applications?
        if (!ApplicationConfig.isApplicationModeEnabled()) {
            return;
        }
        for (App application : apps.applications()) {
            loadPluginApplication(application);
        }
    }

    private void loadPluginApplication(App app) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final ApplicationState state = new ApplicationState(applicationListener, app.id(), app.name());
            app.insertInto(new ApplicationStateConsumer(state));
            int numExports = state.listFields().size();
            log.info().append("\tfound ").append(numExports).append(" exports").endl();
            applications.onApplicationLoad(state);
        }
    }

    private static class ApplicationStateConsumer extends ConsumerBase {
        private final ApplicationState state;

        public ApplicationStateConsumer(ApplicationState state) {
            this.state = Objects.requireNonNull(state);
        }

        @Override
        public void set(String name, Object object) {
            state.setField(name, object);
        }

        @Override
        public void set(String name, Object object, String description) {
            state.setField(name, object, description);
        }
    }
}
