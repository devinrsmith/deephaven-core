package io.deephaven.server.auth;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.AuthContext.SuperUser;
import io.deephaven.auth.ServiceAuthWiring;
import io.deephaven.auth.codegen.impl.ApplicationServiceAuthWiring;
import io.deephaven.auth.codegen.impl.ConfigServiceAuthWiring;
import io.deephaven.auth.codegen.impl.ConsoleServiceAuthWiring;
import io.deephaven.auth.codegen.impl.HealthAuthWiring;
import io.deephaven.auth.codegen.impl.InputTableServiceContextualAuthWiring;
import io.deephaven.auth.codegen.impl.ObjectServiceAuthWiring;
import io.deephaven.auth.codegen.impl.PartitionedTableServiceContextualAuthWiring;
import io.deephaven.auth.codegen.impl.SessionServiceAuthWiring;
import io.deephaven.auth.codegen.impl.StorageServiceAuthWiring;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.backplane.script.grpc.CancelCommandRequest;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest;
import io.deephaven.proto.backplane.script.grpc.GetConsoleTypesRequest;
import io.deephaven.proto.backplane.script.grpc.GetHeapInfoRequest;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.proto.backplane.script.grpc.StartConsoleRequest;
import io.deephaven.server.session.TicketResolverBase.AuthTransformation;
import io.deephaven.server.session.TicketResolverBase.IdentityTransformation;

public final class MyAuthorization implements AuthorizationProvider {
    @Override
    public ApplicationServiceAuthWiring getApplicationServiceAuthWiring() {
        return new ApplicationServiceAuthWiring.DenyAll();
    }

    @Override
    public ConfigServiceAuthWiring getConfigServiceAuthWiring() {
        return new ConfigServiceAuthWiring.DenyAll();
    }

    @Override
    public ConsoleServiceAuthWiring getConsoleServiceAuthWiring() {
        return new ConsoleServiceAuthWiring.DenyAll() {
            @Override
            public void onMessageReceivedGetConsoleTypes(AuthContext authContext, GetConsoleTypesRequest request) {
                // allowed
            }

            @Override
            public void onMessageReceivedGetHeapInfo(AuthContext authContext, GetHeapInfoRequest request) {
                // allowed
            }
        };
    }

    @Override
    public ObjectServiceAuthWiring getObjectServiceAuthWiring() {
        return new ObjectServiceAuthWiring.DenyAll();
    }

    @Override
    public SessionServiceAuthWiring getSessionServiceAuthWiring() {
        return new SessionServiceAuthWiring.AllowAll();
    }

    @Override
    public StorageServiceAuthWiring getStorageServiceAuthWiring() {
        return new StorageServiceAuthWiring.DenyAll();
    }

    @Override
    public HealthAuthWiring getHealthAuthWiring() {
        return new HealthAuthWiring.DenyAll();
    }

    @Override
    public TableServiceContextualAuthWiring getTableServiceContextualAuthWiring() {
        return new TableServiceContextualAuthWiring.DenyAll();
    }

    @Override
    public InputTableServiceContextualAuthWiring getInputTableServiceContextualAuthWiring() {
        return new InputTableServiceContextualAuthWiring.DenyAll();
    }

    @Override
    public PartitionedTableServiceContextualAuthWiring getPartitionedTableServiceContextualAuthWiring() {
        return new PartitionedTableServiceContextualAuthWiring.DenyAll();
    }

    @Override
    public AuthTransformation getTicketTransformation() {
        return IdentityTransformation.INSTANCE;
    }

    @Override
    public AuthContext getInstanceAuthContext() {
        return new SuperUser(); // todo
    }
}
