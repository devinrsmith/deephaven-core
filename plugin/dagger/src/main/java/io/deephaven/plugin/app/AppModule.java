package io.deephaven.plugin.app;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides an empty set for {@link App}.
 *
 * <p>
 * Adapts {@link State states} into {@link App apps}.
 *
 * @see StateModule
 */
@Module(includes = {StateModule.class})
public interface AppModule {

    @Provides
    @ElementsIntoSet
    static Set<App> primesApps() {
        return Collections.emptySet();
    }

    @Provides
    @ElementsIntoSet
    static Set<App> adaptsStates(Set<State> states) {
        return states.stream().map(StateAdapter::new).collect(Collectors.toSet());
    }
}
