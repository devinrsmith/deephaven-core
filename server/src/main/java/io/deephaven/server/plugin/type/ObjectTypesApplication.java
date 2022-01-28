package io.deephaven.server.plugin.type;

import io.deephaven.plugin.app.ApplicationDelegate;

import javax.inject.Inject;

public final class ObjectTypesApplication extends ApplicationDelegate {

    @Inject
    public ObjectTypesApplication(ObjectTypesTable objectTypesTable) {
        super(
                ObjectTypesApplication.class,
                objectTypesTable,
                ReservedTypes.INSTANCE);
    }
}
