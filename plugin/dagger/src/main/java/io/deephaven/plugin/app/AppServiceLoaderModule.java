package io.deephaven.plugin.app;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

@Module(includes = {AppModule.class})
public interface AppServiceLoaderModule {

    @Provides
    @ElementsIntoSet
    static Set<App> providesServiceLoaderApps() {
        return ServiceLoader.load(App.class).stream().map(Provider::get).collect(Collectors.toSet());
    }
}
