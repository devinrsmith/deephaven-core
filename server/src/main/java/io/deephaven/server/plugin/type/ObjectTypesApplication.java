package io.deephaven.server.plugin.type;

import io.deephaven.plugin.app.AppStateApp;

import javax.inject.Inject;

public final class ObjectTypesApplication extends AppStateApp {

    @Inject
    public ObjectTypesApplication(ObjectTypes objectTypes) {
        super(ObjectTypesApplication.class.getName(), "ObjectTypes Information", objectTypes);
    }
}
