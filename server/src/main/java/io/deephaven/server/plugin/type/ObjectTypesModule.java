package io.deephaven.server.plugin.type;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.plugin.type.ObjectTypeRegistration;
import io.deephaven.plugin.type.ObjectTypeRegistration.Listener;

import java.util.Collections;
import java.util.Set;

/**
 * Binds {@link ObjectTypes} as {@link ObjectTypeLookup} and {@link ObjectTypeRegistration}.
 */
@Module(includes = {ObjectTypesApplicationModule.class})
public interface ObjectTypesModule {

    @Provides
    @ElementsIntoSet
    static Set<Listener> primesListeners() {
        return Collections.emptySet();
    }

    @Binds
    ObjectTypeLookup bindsLookup(ObjectTypes types);

    @Binds
    ObjectTypeRegistration bindsCallback(ObjectTypes types);
}
