package io.deephaven.server.appmode;

import io.deephaven.plugin.app.AppStateApp;

import javax.inject.Inject;

public final class ApplicationApplication extends AppStateApp {

    @Inject
    public ApplicationApplication(Applications applications) {
        super(ApplicationApplication.class.getName(), "Application Information", applications);
    }
}
