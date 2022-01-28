package io.deephaven.server.appmode;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.plugin.app.App;

import javax.inject.Singleton;

/**
 * Provides stuff to build {@link ApplicationApplication}.
 */
@Module
public interface ApplicationApplicationModule {

    @Binds
    @IntoSet
    App providesApplication(ApplicationApplication plugin);

    @Provides
    @Singleton
    static ApplicationTable providesApplicationTable() {
        return ApplicationTable.create();
    }

    @Provides
    @Singleton
    static FieldTable providesFieldTable() {
        return FieldTable.create();
    }

    @Binds
    @IntoSet
    Applications.Listener bindsApplicationTableListener(ApplicationTable applicationTable);

    @Binds
    @IntoSet
    ApplicationState.Listener bindsApplicationFieldListener(FieldListener listener);
}
