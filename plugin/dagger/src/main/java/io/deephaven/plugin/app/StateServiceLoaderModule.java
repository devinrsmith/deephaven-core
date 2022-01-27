package io.deephaven.plugin.app;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the {@link ServiceLoader#load(Class)} set for {@link State}.
 *
 * @see StateModule
 */
@Module(includes = {StateModule.class})
public interface StateServiceLoaderModule {

    @Provides
    @ElementsIntoSet
    static Set<State> providesServiceLoaderStates() {
        return ServiceLoader.load(State.class).stream().map(Provider::get).collect(Collectors.toSet());
    }
}
