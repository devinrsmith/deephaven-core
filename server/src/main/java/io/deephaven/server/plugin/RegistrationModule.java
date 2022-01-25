package io.deephaven.server.plugin;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.plugin.Registration;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Binds all {@link Registration} found via {@link ServiceLoader#load(Class)} with {@link ElementsIntoSet}.
 */
@Module
public interface RegistrationModule {

    @Provides
    @ElementsIntoSet
    static Set<Registration> provideServiceLoaderRegistrations() {
        return ServiceLoader
                .load(Registration.class)
                .stream()
                .map(Provider::get)
                .collect(Collectors.toSet());
    }
}
