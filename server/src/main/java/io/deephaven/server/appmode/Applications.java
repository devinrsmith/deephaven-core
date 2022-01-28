package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.plugin.app.StateDelegate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

final class Applications implements ApplicationStates {

    public static Applications create() {
        return new Applications(new ConcurrentHashMap<>(), ApplicationTable.create());
    }

    private final Map<String, ApplicationState> applicationMap;
    private final ApplicationTable table;

    private Applications(Map<String, ApplicationState> applicationMap, ApplicationTable table) {
        this.applicationMap = Objects.requireNonNull(applicationMap);
        this.table = Objects.requireNonNull(table);
    }

    public synchronized void onApplicationLoad(final ApplicationState app) {
        if (applicationMap.containsKey(app.id())) {
            if (applicationMap.get(app.id()) != app) {
                throw new IllegalArgumentException("Duplicate application found for app_id " + app.id());
            }
            return;
        }
        applicationMap.put(app.id(), app);
        try {
            table.add(app.id(), app.name());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ApplicationTable table() {
        return table;
    }

    @Override
    public Optional<ApplicationState> getApplicationState(String applicationId) {
        return Optional.ofNullable(applicationMap.get(applicationId));
    }

    @Override
    public Collection<ApplicationState> values() {
        return Collections.unmodifiableCollection(applicationMap.values());
    }

}
