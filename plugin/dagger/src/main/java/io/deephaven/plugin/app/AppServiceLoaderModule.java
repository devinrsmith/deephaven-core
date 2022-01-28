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
        return ServiceLoader.load(App.class).stream().map(AppServiceLoader::new).collect(Collectors.toSet());
    }

    final class AppServiceLoader extends AppRealBase {

        private final Provider<App> provider;
        private final Class<? extends App> type;

        public AppServiceLoader(ServiceLoader.Provider<App> provider) {
            this.provider = DoubleCheck.provider(provider::get);
            this.type = provider.type();
        }

        public Class<? extends App> type() {
            return type;
        }

        @Override
        public String id() {
            return provider.get().name();
        }

        @Override
        public String name() {
            return provider.get().name();
        }

        @Override
        public void insertInto(Consumer consumer) {
            provider.get().insertInto(consumer);
        }

        @Override
        public String toString() {
            return "ServiceLoaderApp(" + type.getName() + ")";
        }
    }
}
