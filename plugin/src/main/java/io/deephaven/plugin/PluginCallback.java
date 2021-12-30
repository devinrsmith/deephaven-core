package io.deephaven.plugin;

import io.deephaven.plugin.application.ApplicationInfo;
import io.deephaven.plugin.type.ObjectType;

public interface PluginCallback {

    void registerObjectType(ObjectType objectType);

    void registerApplication(ApplicationInfo applicationInfo);
}
