package io.deephaven.plugin.app;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;

import java.util.Collections;
import java.util.Set;

/**
 * Provides an empty set for {@link State}.
 */
@Module
public interface StateModule {

    @Provides
    @ElementsIntoSet
    static Set<State> primesStates() {
        return Collections.emptySet();
    }
}
