package io.deephaven.grpc_api.uri;

import io.deephaven.util.auth.AuthContext;

import javax.inject.Inject;

public final class ParquetTableResolverSuperUser extends ParquetTableResolver {

    @Inject
    public ParquetTableResolverSuperUser() {

    }

    @Override
    public Authorization<String> authorization(AuthorizationScope<String> scope, AuthContext context) {
        if (scope.isWrite()) {
            return Authorization.deny(scope, "The Parquet resolver does not allow publishing");
        }
        if (context == null || !context.isSuperUser()) {
            return Authorization.deny(scope, "The Parquet resolver requires a super-user");
        }
        return Authorization.allow(scope);
    }
}
