package io.deephaven;

import com.google.auto.service.AutoService;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Listener;

@AutoService(ApplicationState.Factory.class)
public final class MemoryApplication implements ApplicationState.Factory {

    @Override
    public ApplicationState create(Listener appStateListener) {
        return new ApplicationState(appStateListener, MemoryApplication.class.getName(), "Memory Application");
    }
}
