package io.deephaven.server.plugin.type;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.plugin.type.ObjectTypeRegistration;

/**
 * Binds {@link ObjectTypes} as {@link ObjectTypeLookup} and {@link ObjectTypeRegistration}.
 */
@Module
public interface ObjectTypesModule {

    @Binds
    ObjectTypeLookup bindsLookup(ObjectTypes types);

    @Binds
    ObjectTypeRegistration bindsCallback(ObjectTypes types);

    @Binds
    @IntoSet
    App bindsApplication(ObjectTypesApplication application);
}
