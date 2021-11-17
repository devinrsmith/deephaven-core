package io.deephaven.grpc_api.uri;

import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public final class CsvTableResolverSuperUser extends CsvTableResolver {

    @Inject
    public CsvTableResolverSuperUser() {}

    @Override
    public boolean isEnabled(AuthContext auth) {
        return auth.isSuperUser();
    }

    @Override
    public boolean isEnabled(AuthContext auth, String item) {
        return true;
    }

    @Override
    public String helpEnable(AuthContext auth) {
        return "Enabled for super-users.";
    }

    @Override
    public String helpEnable(AuthContext auth, String item) {
        throw new IllegalStateException();
    }
}
