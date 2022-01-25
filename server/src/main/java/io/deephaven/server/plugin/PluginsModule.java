package io.deephaven.server.plugin;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.Registration.Callback;
import io.deephaven.server.plugin.type.ObjectTypesModule;

import java.util.Collections;
import java.util.Set;

/**
 * Includes the {@link Module modules} necessary to provide {@link PluginsAutoDiscovery}.
 *
 * <p>
 * Note: runtime plugin registration is not currently supported - ie, no {@link Callback} is provided. See
 * <a href="https://github.com/deephaven/deephaven-core/issues/1809">deephaven-core#1809</a> for the feature request.
 *
 * @see ObjectTypesModule
 * @see RegistrationModule
 */
@Module(includes = {ObjectTypesModule.class, RegistrationModule.class})
public interface PluginsModule {

    @Provides
    @ElementsIntoSet
    static Set<Registration> primeRegistrations() {
        return Collections.emptySet();
    }

    @Binds
    Registration.Callback bindPluginRegistrationCallback(PluginRegistrationVisitor visitor);
}
