package io.deephaven.grpc_api.uri;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;
import io.deephaven.grpc_api.appmode.ApplicationStates;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * The application table resolver is able to resolve {@link ApplicationUri application URIs}.
 *
 * <p>
 * For example, {@code dh:///app/my_app/field/my_field}.
 *
 * @see ApplicationUri application URI format
 */
public final class ApplicationResolver extends UriResolverDeephavenBase<ApplicationUri> {

    public static ApplicationResolver get() {
        return UriRouterInstance.get().find(ApplicationResolver.class).get();
    }

    private final ApplicationStates states;

    @Inject
    public ApplicationResolver(ApplicationStates states, Config<ApplicationUri> config) {
        super(config, ApplicationUri::of);
        this.states = Objects.requireNonNull(states);
    }

    @Override
    public Set<String> schemes() {
        return Collections.singleton(DeephavenUri.LOCAL_SCHEME);
    }

    @Override
    public boolean isResolvable(URI uri) {
        return ApplicationUri.isWellFormed(uri);
    }

    @Override
    public Object resolve(AuthContext auth, ApplicationUri uri) {
        final Field<Object> field = getField(uri);
        return field == null ? null : field.value();
    }

    public Field<Object> getField(ApplicationUri uri) {
        return getField(uri.applicationId(), uri.fieldName());
    }

    public Field<Object> getField(String applicationId, String fieldName) {
        final ApplicationState app = states.getApplicationState(applicationId).orElse(null);
        return app == null ? null : app.getField(fieldName);
    }
}
