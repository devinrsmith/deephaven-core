package io.deephaven.plugin.app;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the {@link ServiceLoader#load(Class)} set for {@link App}.
 *
 * @see AppModule
 * @see StateServiceLoaderModule
 */
@Module(includes = {AppModule.class, StateServiceLoaderModule.class})
public interface AppServiceLoaderModule {

    @Provides
    @ElementsIntoSet
    static Set<App> providesServiceLoaderApps() {
        return ServiceLoader.load(App.class).stream().map(Provider::get).collect(Collectors.toSet());
    }
}
