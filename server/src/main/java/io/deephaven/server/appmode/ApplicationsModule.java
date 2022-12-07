/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.appmode;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.app.GcApplication;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.server.auth.MyApplication;

@Module
public interface ApplicationsModule {

    @Provides
    @IntoSet
    static ApplicationState.Factory providesGcApplication() {
        return new GcApplication();
    }

    @Provides
    @IntoSet
    static ApplicationState.Factory providesMyApplication() {
        return new MyApplication();
    }
}
