package io.deephaven.server.plugin.type;

import io.deephaven.plugin.app.AppImpl;
import io.deephaven.plugin.app.NamedStates;

import javax.inject.Inject;

public final class ObjectTypesApplication extends AppImpl {

    @Inject
    public ObjectTypesApplication(ObjectTypes objectTypes) {
        super(
                ObjectTypesApplication.class,
                NamedStates.of("objectType", objectTypes));
    }
}
