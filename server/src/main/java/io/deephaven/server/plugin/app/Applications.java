package io.deephaven.server.plugin.app;

import io.deephaven.plugin.app.App;
import io.deephaven.plugin.app.AppLookup;
import io.deephaven.plugin.app.AppRegistration;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

@Singleton
public final class Applications implements AppRegistration, AppLookup {

    private final List<App> apps;

    @Inject
    public Applications() {
        apps = new ArrayList<>();
    }

    @Override
    public synchronized void register(App app) {
        apps.add(app);
    }

    @Override
    public synchronized List<App> applications() {
        return new ArrayList<>(apps);
    }
}
