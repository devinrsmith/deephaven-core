package io.deephaven.plugin.app;

import dagger.Module;
import dagger.Provides;
import dagger.internal.DoubleCheck;
import dagger.multibindings.ElementsIntoSet;

import javax.inject.Provider;
import java.util.ServiceLoader;
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
        return ServiceLoader.load(State.class).stream().map(StateServiceLoader::new).collect(Collectors.toSet());
    }

    final class StateServiceLoader implements State {
        private final Provider<State> provider;
        private final Class<? extends State> type;

        public StateServiceLoader(ServiceLoader.Provider<State> provider) {
            this.provider = DoubleCheck.provider(provider::get);
            this.type = provider.type();
        }

        public Class<? extends State> type() {
            return type;
        }

        @Override
        public void insertInto(Consumer consumer) {
            provider.get().insertInto(consumer);
        }

        @Override
        public String toString() {
            return "ServiceLoaderState(" + type.getName() + ")";
        }
    }
}
