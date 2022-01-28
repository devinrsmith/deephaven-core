package io.deephaven.server.plugin.type;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.plugin.app.App;
import io.deephaven.plugin.type.ObjectTypeRegistration.Listener;

import javax.inject.Singleton;

@Module
public interface ObjectTypesApplicationModule {

    @Binds
    @IntoSet
    App bindsObjectTypesApplication(ObjectTypesApplication application);

    @Binds
    @IntoSet
    Listener bindsListener(ObjectTypesTable table);

    @Provides
    @Singleton
    static ObjectTypesTable providesObjectTypesTable() {
        return ObjectTypesTable.create();
    }
}
