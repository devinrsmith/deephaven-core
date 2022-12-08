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
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.ApplyPreviewColumnsRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
import io.deephaven.proto.backplane.grpc.FetchFileRequest;
import io.deephaven.proto.backplane.grpc.FetchTableRequest;
import io.deephaven.proto.backplane.grpc.FlattenRequest;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.proto.backplane.grpc.ListItemsRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
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
import io.deephaven.util.SafeCloseable;

import java.util.List;

public final class MyAuthorization implements AuthorizationProvider {
    @Override
    public ApplicationServiceAuthWiring getApplicationServiceAuthWiring() {
        // todo: web ui needs this
        return new ApplicationServiceAuthWiring.AllowAll();
    }

    @Override
    public ConfigServiceAuthWiring getConfigServiceAuthWiring() {
        // todo: web ui needs this
        return new ConfigServiceAuthWiring.AllowAll();
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

            @Override
            public void onMessageReceivedStartConsole(AuthContext authContext, StartConsoleRequest request) {
                // allowed
            }

            @Override
            public void onMessageReceivedSubscribeToLogs(AuthContext authContext, LogSubscriptionRequest request) {
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
        return new StorageServiceAuthWiring.DenyAll() {
            @Override
            public void onMessageReceivedListItems(AuthContext authContext, ListItemsRequest request) {
                // todo: web ui needs this
            }

            @Override
            public void onMessageReceivedFetchFile(AuthContext authContext, FetchFileRequest request) {
                // todo: web ui needs this
            }
        };
    }

    @Override
    public HealthAuthWiring getHealthAuthWiring() {
        return new HealthAuthWiring.AllowAll();
    }

    @Override
    public TableServiceContextualAuthWiring getTableServiceContextualAuthWiring() {
        return new TableServiceContextualAuthWiring.DenyAll() {
            @Override
            public void checkPermissionExportedTableUpdates(AuthContext authContext,
                    ExportedTableUpdatesRequest request, List<Table> sourceTables) {
                // todo: web ui needs this
            }

            @Override
            public void checkPermissionApplyPreviewColumns(AuthContext authContext, ApplyPreviewColumnsRequest request,
                    List<Table> sourceTables) {
                // todo: web ui needs this
            }

            @Override
            public void checkPermissionFlatten(AuthContext authContext, FlattenRequest request,
                    List<Table> sourceTables) {
                // todo: web ui needs this
            }

            @Override
            public void checkPermissionFetchTable(AuthContext authContext, FetchTableRequest request,
                    List<Table> sourceTables) {
                // my application needs this for w2w
                // doesn't technically prevent it, but it's how w2w works right now
                try (final SafeCloseable _lock = UpdateGraphProcessor.DEFAULT.sharedLock().lockCloseable()) {
                    if (sourceTables.get(0).size() > 1_000_000) {
                        ServiceAuthWiring.operationNotAllowed("Preventing fetch, table too big for worker-2-worker");
                    }
                }
            }

            // @Override
            // public void checkPermissionView(AuthContext authContext, SelectOrUpdateRequest request, List<Table>
            // sourceTables) {
            // // allow temp
            // }
            //
            // @Override
            // public void checkPermissionEmptyTable(AuthContext authContext, EmptyTableRequest request, List<Table>
            // sourceTables) {
            // // allow temp
            // }
        };
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
