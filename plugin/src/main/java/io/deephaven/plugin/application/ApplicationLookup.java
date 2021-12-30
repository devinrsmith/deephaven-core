package io.deephaven.plugin.application;

import java.util.Optional;

public interface ApplicationLookup extends Iterable<ApplicationInfo> {

    Optional<ApplicationInfo> findApplicationInfo(String id);
}
