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
import java.util.function.BiConsumer;
import java.util.function.Predicate;

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
    public final ApplicationUri adaptToPath(URI uri) {
        return ApplicationUri.of(uri);
    }

    @Override
    public final URI adaptToUri(ApplicationUri path) {
        return path.toURI();
    }

    @Override
    public final Object resolvePath(ApplicationUri path) {
        final Field<Object> field = getField(path);
        return field == null ? null : field.value();
    }

    @Override
    public final void forAllPaths(BiConsumer<ApplicationUri, Object> consumer) {
        states.forEach(new Adapter(null, consumer));
    }

    @Override
    public final void forPaths(Predicate<ApplicationUri> predicate, BiConsumer<ApplicationUri, Object> consumer) {
        states.forEach(new Adapter(Objects.requireNonNull(predicate), consumer));
    }

    public final Field<Object> getField(ApplicationUri uri) {
        return getField(uri.applicationId(), uri.fieldName());
    }

    public final Field<Object> getField(String applicationId, String fieldName) {
        final ApplicationState app = states.getApplicationState(applicationId).orElse(null);
        return app == null ? null : app.getField(fieldName);
    }

    private static class Adapter implements BiConsumer<ApplicationState, Field<?>> {
        private final Predicate<ApplicationUri> predicate;
        private final BiConsumer<ApplicationUri, Object> delegate;

        Adapter(Predicate<ApplicationUri> predicate, BiConsumer<ApplicationUri, Object> delegate) {
            this.predicate = predicate;
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public void accept(ApplicationState applicationState, Field<?> field) {
            final ApplicationUri uri = ApplicationUri.of(applicationState.id(), field.name());
            if (predicate == null || predicate.test(uri)) {
                delegate.accept(uri, field.value());
            }
        }
    }
}
