package io.deephaven.server.appmode;

import io.deephaven.plugin.app.ApplicationDelegate;

import javax.inject.Inject;

public final class ApplicationApplication extends ApplicationDelegate {

    @Inject
    public ApplicationApplication(ApplicationTable applicationTable, FieldTable fieldTable) {
        super(
                ApplicationApplication.class,
                applicationTable,
                fieldTable);
    }
}
