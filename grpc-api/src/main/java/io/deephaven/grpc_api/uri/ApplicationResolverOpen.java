package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.appmode.ApplicationStates;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public final class ApplicationResolverOpen extends ApplicationResolver {
    @Inject
    public ApplicationResolverOpen(ApplicationStates states) {
        super(states);
    }

    @Override
    public boolean isEnabled(AuthContext auth) {
        return true;
    }

    @Override
    public boolean isEnabled(AuthContext auth, ApplicationUri item) {
        return !item.fieldName().startsWith("__");
    }

    @Override
    public String helpEnable(AuthContext auth) {
        throw new IllegalStateException();
    }

    @Override
    public String helpEnable(AuthContext auth, ApplicationUri item) {
        return "Unable to resolve field names that start with '__'.";
    }
}
