package io.deephaven.server.auth;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.AuthContext.SuperUser;
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
                super.onMessageReceivedGetConsoleTypes(authContext, request);
            }

            @Override
            public void onMessageReceivedStartConsole(AuthContext authContext, StartConsoleRequest request) {
                super.onMessageReceivedStartConsole(authContext, request);
            }

            @Override
            public void onMessageReceivedGetHeapInfo(AuthContext authContext, GetHeapInfoRequest request) {
                super.onMessageReceivedGetHeapInfo(authContext, request);
            }

            @Override
            public void onMessageReceivedSubscribeToLogs(AuthContext authContext, LogSubscriptionRequest request) {
                super.onMessageReceivedSubscribeToLogs(authContext, request);
            }

            @Override
            public void onMessageReceivedExecuteCommand(AuthContext authContext, ExecuteCommandRequest request) {
                super.onMessageReceivedExecuteCommand(authContext, request);
            }

            @Override
            public void onMessageReceivedCancelCommand(AuthContext authContext, CancelCommandRequest request) {
                super.onMessageReceivedCancelCommand(authContext, request);
            }

            @Override
            public void onMessageReceivedBindTableToVariable(AuthContext authContext, BindTableToVariableRequest request) {
                super.onMessageReceivedBindTableToVariable(authContext, request);
            }

            @Override
            public void onCallStartedAutoCompleteStream(AuthContext authContext) {
                super.onCallStartedAutoCompleteStream(authContext);
            }

            @Override
            public void onMessageReceivedAutoCompleteStream(AuthContext authContext, AutoCompleteRequest request) {
                super.onMessageReceivedAutoCompleteStream(authContext, request);
            }

            @Override
            public void onMessageReceivedOpenAutoCompleteStream(AuthContext authContext, AutoCompleteRequest request) {
                super.onMessageReceivedOpenAutoCompleteStream(authContext, request);
            }

            @Override
            public void onMessageReceivedNextAutoCompleteStream(AuthContext authContext, AutoCompleteRequest request) {
                super.onMessageReceivedNextAutoCompleteStream(authContext, request);
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
