package io.deephaven.server.plugin.type;

import io.deephaven.plugin.app.AppStates;

import javax.inject.Inject;

public final class ObjectTypesApplication extends AppStates {

    @Inject
    public ObjectTypesApplication(ObjectTypes objectTypes) {
        super(ObjectTypesApplication.class.getName(), "ObjectTypes Information", objectTypes);
    }
}
