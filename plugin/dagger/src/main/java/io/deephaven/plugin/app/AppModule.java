package io.deephaven.plugin.app;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;

import java.util.Collections;
import java.util.Set;

@Module
public interface AppModule {

    @Provides
    @ElementsIntoSet
    static Set<App> primesApps() {
        return Collections.emptySet();
    }
}
