package io.deephaven.server.plugin;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.AppRegistration;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeRegistration;

import javax.inject.Inject;
import java.util.Objects;

final class PluginRegistrationVisitor
        implements io.deephaven.plugin.Registration.Callback, Plugin.Visitor<PluginRegistrationVisitor> {

    private final ObjectTypeRegistration objectTypeRegistration;
    private final AppRegistration appRegistration;

    @Inject
    public PluginRegistrationVisitor(ObjectTypeRegistration objectTypeRegistration, AppRegistration appRegistration) {
        this.objectTypeRegistration = Objects.requireNonNull(objectTypeRegistration);
        this.appRegistration = Objects.requireNonNull(appRegistration);
    }

    @Override
    public void register(Plugin plugin) {
        plugin.walk(this);
    }

    @Override
    public PluginRegistrationVisitor visit(ObjectType objectType) {
        objectTypeRegistration.register(objectType);
        return this;
    }

    @Override
    public PluginRegistrationVisitor visit(App app) {
        appRegistration.register(app);
        return this;
    }
}
