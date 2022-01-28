package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
final class Applications implements ApplicationStates {

    interface Listener {
        void onApplicationLoad(final ApplicationState app);
    }

    private final Map<String, ApplicationState> applicationMap;
    private final Set<Listener> listeners;

    @Inject
    public Applications(Set<Listener> listeners) {
        this.applicationMap = new ConcurrentHashMap<>();
        this.listeners = Objects.requireNonNull(listeners);
    }

    public synchronized void onApplicationLoad(final ApplicationState app) {
        if (applicationMap.containsKey(app.id())) {
            if (applicationMap.get(app.id()) != app) {
                throw new IllegalArgumentException("Duplicate application found for app_id " + app.id());
            }
            return;
        }
        applicationMap.put(app.id(), app);
        for (Listener listener : listeners) {
            listener.onApplicationLoad(app);
        }
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
