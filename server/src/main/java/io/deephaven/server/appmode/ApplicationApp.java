package io.deephaven.server.appmode;

import io.deephaven.plugin.app.AppImpl;
import io.deephaven.plugin.app.NamedStates;

import javax.inject.Inject;

public final class ApplicationApp extends AppImpl {

    @Inject
    public ApplicationApp(Applications applications, FieldState fields) {
        super(ApplicationApp.class, NamedStates.of("test", applications, fields));
    }
}
