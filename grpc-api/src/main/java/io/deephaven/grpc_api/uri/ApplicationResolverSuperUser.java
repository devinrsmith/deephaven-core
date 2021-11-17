package io.deephaven.grpc_api.uri;

import io.deephaven.grpc_api.appmode.ApplicationStates;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public final class ApplicationResolverSuperUser extends ApplicationResolver {
    @Inject
    public ApplicationResolverSuperUser(ApplicationStates states) {
        super(states);
    }

    @Override
    public boolean isEnabled(AuthContext auth) {
        return auth.isSuperUser();
    }

    @Override
    public boolean isEnabled(AuthContext auth, ApplicationUri uri) {
        return true;
    }

    @Override
    public String helpEnable(AuthContext auth) {
        return "Enabled for super-users.";
    }

    @Override
    public String helpEnable(AuthContext auth, ApplicationUri uri) {
        throw new IllegalStateException();
    }
}
