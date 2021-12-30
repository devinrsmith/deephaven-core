package io.deephaven.server.plugin;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.plugin.application.ApplicationLookup;

import javax.inject.Singleton;

@Module
public interface ApplicationInfosModule {

    @Provides
    @Singleton
    static ApplicationInfos providesApplicationInfos() {
        return new ApplicationInfos();
    }

    @Binds
    ApplicationLookup providesLookup(ApplicationInfos types);
}
