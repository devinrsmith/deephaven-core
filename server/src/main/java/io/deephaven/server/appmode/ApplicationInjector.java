package io.deephaven.server.appmode;

import io.deephaven.appmode.Application;
import io.deephaven.appmode.Application.Builder;
import io.deephaven.appmode.ApplicationConfig;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Fields;
import io.deephaven.appmode.StandardField;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.plugin.application.ApplicationInfo;
import io.deephaven.plugin.application.ApplicationInfo.Script;
import io.deephaven.plugin.application.ApplicationInfo.State;
import io.deephaven.plugin.application.ApplicationLookup;
import io.deephaven.server.console.GlobalSessionProvider;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ApplicationInjector {

    private static final Logger log = LoggerFactory.getLogger(ApplicationInjector.class);

    private final AppMode appMode;
    private final GlobalSessionProvider globalSessionProvider;
    private final ApplicationTicketResolver ticketResolver;
    private final ApplicationState.Listener applicationListener;

    private final ApplicationLookup applicationLookup;

    @Inject
    public ApplicationInjector(final AppMode appMode,
            final GlobalSessionProvider globalSessionProvider,
            final ApplicationTicketResolver ticketResolver,
            final ApplicationState.Listener applicationListener,
            final ApplicationLookup applicationLookup) {
        this.appMode = appMode;
        this.globalSessionProvider = Objects.requireNonNull(globalSessionProvider);
        this.ticketResolver = ticketResolver;
        this.applicationListener = applicationListener;
        this.applicationLookup = applicationLookup;
    }

    public void run() throws IOException, ClassNotFoundException {
        if (!ApplicationConfig.isApplicationModeEnabled()) {
            return;
        }

        final Path applicationDir = ApplicationConfig.applicationDir();
        log.info().append("Finding application(s) in '").append(applicationDir.toString()).append("'...").endl();

        List<ApplicationConfig> configs = new ArrayList<>();
        try {
            configs.addAll(ApplicationConfig.find());
        } catch (final NoSuchFileException ignored) {
            // ignore
        }

        for (ApplicationInfo applicationInfo : applicationLookup) {
            final Builder builder = Application.builder()
                    .id(applicationInfo.id())
                    .name(applicationInfo.name());
            final Script script = applicationInfo.script();
            final Fields.Builder fields = Fields.builder();
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                script.initializeApplication(new State() {
                    @Override
                    public <T> void setField(String name, T value) {
                        fields.putFields(name, StandardField.of(name, value));
                    }
                });
            }
            final Application app = builder.fields(fields.build()).build();
            final ApplicationState state = app.toState(applicationListener);
            ticketResolver.onApplicationLoad(state);
        }

        if (configs.isEmpty()) {
            log.warn().append("No application(s) found...").endl();
            if (appMode != AppMode.HYBRID) {
                log.warn().append("No console sessions allowed...").endl();
            }
            return;
        }

        for (ApplicationConfig config : configs) {
            if (!config.isEnabled()) {
                log.info().append("Skipping disabled application: ").append(config.toString()).endl();
                continue;
            }
            loadApplication(applicationDir, config);
        }
    }

    private void loadApplication(final Path applicationDir, final ApplicationConfig config) {
        // Note: if we need to be more specific about which application we are starting, we can print out the path of
        // the application.
        log.info().append("Starting application '").append(config.toString()).append('\'').endl();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final ApplicationState app = ApplicationFactory.create(applicationDir, config,
                    globalSessionProvider.getGlobalSession(), applicationListener);

            int numExports = app.listFields().size();
            log.info().append("\tfound ").append(numExports).append(" exports").endl();

            ticketResolver.onApplicationLoad(app);
        }
    }
}
