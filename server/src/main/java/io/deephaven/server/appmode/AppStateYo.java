package io.deephaven.server.appmode;

import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.App.Consumer;

import javax.inject.Inject;
import java.util.Objects;

public final class AppStateYo implements App.State {

    private final Applications applications;
    private final FieldState fields;

    @Inject
    public AppStateYo(Applications applications, FieldState fields) {
        this.applications = Objects.requireNonNull(applications);
        this.fields = Objects.requireNonNull(fields);
    }

    @Override
    public void insertInto(Consumer consumer) {
        consumer.set("applications", applications.table(), "Applications");
        consumer.set("fields", fields.table(), "Fields");
    }
}
