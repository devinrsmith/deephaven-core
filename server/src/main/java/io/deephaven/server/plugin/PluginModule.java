package io.deephaven.server.plugin;

import dagger.Module;

@Module(includes = { ObjectTypesModule.class, ApplicationInfosModule.class })
public interface PluginModule {

}
