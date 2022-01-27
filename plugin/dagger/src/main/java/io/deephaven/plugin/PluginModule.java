package io.deephaven.plugin;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.AppModule;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeModule;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Provides an empty set for {@link Plugin plugins}.
 *
 * <p>
 * Adapts {@link App apps} and {@link ObjectType object types} into {@link Plugin plugins}.
 *
 * @see AppModule
 * @see ObjectTypeModule
 */
@Module(includes = {AppModule.class, ObjectTypeModule.class})
public interface PluginModule {

    @Provides
    @ElementsIntoSet
    static Set<Plugin> primesPlugins() {
        return Collections.emptySet();
    }

    @Provides
    @ElementsIntoSet
    static Set<Plugin> adaptsApps(Set<App> apps) {
        return new HashSet<>(apps);
    }

    @Provides
    @ElementsIntoSet
    static Set<Plugin> adaptsObjectTypes(Set<ObjectType> apps) {
        return new HashSet<>(apps);
    }
}
