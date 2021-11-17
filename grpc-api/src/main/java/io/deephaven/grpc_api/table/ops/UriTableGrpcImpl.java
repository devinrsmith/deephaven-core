package io.deephaven.grpc_api.table.ops;

import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.grpc_api.console.ConsoleServiceGrpcImpl;
import io.deephaven.grpc_api.session.SessionState.ExportObject;
import io.deephaven.grpc_api.uri.UriRouter;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.UriTableRequest;
import io.deephaven.uri.UriHelper;
import io.deephaven.util.auth.AuthContext;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.util.List;
import java.util.Objects;

@Singleton
public final class UriTableGrpcImpl extends GrpcTableOperation<UriTableRequest> {

    private final UriRouter uriRouter;

    @Inject()
    public UriTableGrpcImpl(final UriRouter uriRouter) {
        super(BatchTableRequest.Operation::getUriTable, UriTableRequest::getResultId);
        this.uriRouter = Objects.requireNonNull(uriRouter);
    }

    @Override
    public void validateRequest(AuthContext auth, final UriTableRequest request) throws StatusRuntimeException {
        final URI uri;
        try {
            uri = URI.create(request.getUri());
        } catch (IllegalArgumentException e) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    String.format("Not a URI '%s'", request.getUri()));
        }
        if (!UriHelper.isValidViaApi(uri)) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                    "Deephaven local URIs should be resolved via more appropriate APIs '%s'", request.getUri()));
        }
    }

    @Override
    public Table create(AuthContext auth, final UriTableRequest request, final List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 0);

        // https://github.com/deephaven/deephaven-core/issues/1496
        validateRequest(auth, request);

        // TODO: check user auth if has access to console
        final boolean resolveSafely = ConsoleServiceGrpcImpl.REMOTE_CONSOLE_DISABLED;
        final URI uri = URI.create(request.getUri());

        // TODO: pass user auth info along resolveSafely path?

        Object object;
        try {
            object = resolveSafely ? uriRouter.resolveSafely(auth, uri) : uriRouter.resolve(uri);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null; // todo
        }
        if (object == null) {
            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                    String.format("No table found for URI '%s'", request.getUri()));
        }
        if (!(object instanceof Table)) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    String.format("Object for URI '%s' is not a Table, is '%s'", request.getUri(), object.getClass()));
        }
        return (Table) object;
    }
}
