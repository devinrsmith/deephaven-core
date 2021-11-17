package io.deephaven.grpc_api.uri;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;
import io.deephaven.grpc_api.appmode.ApplicationStates;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.uri.DeephavenUri;

import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public abstract class ApplicationResolver extends UriResolverBase<ApplicationUri> {

    public static ApplicationResolver get() {
        return UriRouterInstance.get().find(ApplicationResolver.class).get();
    }

    private final ApplicationStates states;

    public ApplicationResolver(ApplicationStates states) {
        this.states = Objects.requireNonNull(states);
    }

    @Override
    public final Set<String> schemes() {
        return Collections.singleton(DeephavenUri.LOCAL_SCHEME);
    }

    @Override
    public final boolean isResolvable(URI uri) {
        return ApplicationUri.isWellFormed(uri);
    }

    @Override
    public final ApplicationUri adapt(URI uri) {
        return ApplicationUri.of(uri);
    }

    @Override
    public final Object resolveItem(ApplicationUri item) throws InterruptedException {
        final Field<Object> field = getField(item);
        return field == null ? null : field.value();
    }

    public final Field<Object> getField(ApplicationUri uri) {
        return getField(uri.applicationId(), uri.fieldName());
    }

    public final Field<Object> getField(String applicationId, String fieldName) {
        final ApplicationState app = states.getApplicationState(applicationId).orElse(null);
        return app == null ? null : app.getField(fieldName);
    }
}
