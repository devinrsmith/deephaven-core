package io.deephaven.server.plugin.app;

import dagger.Binds;
import dagger.Module;
import io.deephaven.plugin.app.AppLookup;
import io.deephaven.plugin.app.AppRegistration;

@Module
public interface AppModule {

    @Binds
    AppRegistration bindsRegistration(Applications applications);

    @Binds
    AppLookup bindsLookup(Applications applications);
}
