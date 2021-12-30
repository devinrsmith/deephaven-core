package io.deephaven.server.plugin;

import io.deephaven.plugin.application.ApplicationInfo;
import io.deephaven.plugin.application.ApplicationLookup;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;

final class ApplicationInfos implements ApplicationLookup {

    private final Map<String, ApplicationInfo> map;

    ApplicationInfos() {
        map = new HashMap<>();
    }

    void register(ApplicationInfo applicationInfo) {
        final ApplicationInfo existing = map.putIfAbsent(applicationInfo.id(), applicationInfo);
        if (existing != null) {
            throw new IllegalArgumentException("Unable to register application id, already exists: " + applicationInfo.id());
        }
    }

    @Override
    public Optional<ApplicationInfo> findApplicationInfo(String id) {
        return Optional.ofNullable(map.get(id));
    }

    @NotNull
    @Override
    public Iterator<ApplicationInfo> iterator() {
        return map.values().iterator();
    }

    @Override
    public void forEach(Consumer<? super ApplicationInfo> action) {
        map.values().forEach(action);
    }

    @Override
    public Spliterator<ApplicationInfo> spliterator() {
        return map.values().spliterator();
    }
}
