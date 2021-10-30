package io.deephaven.grpc_api.table.ops;

import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.grpc_api.console.ConsoleServiceGrpcImpl;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.uri.UriResolvers;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.UriTableRequest;
import io.deephaven.uri.UriHelper;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.util.List;
import java.util.Objects;

@Singleton
public final class UriTableGrpcImpl extends GrpcTableOperation<UriTableRequest> {

    private final UriResolvers uriResolvers;

    @Inject()
    public UriTableGrpcImpl(final UriResolvers uriResolvers) {
        super(BatchTableRequest.Operation::getUriTable, UriTableRequest::getResultId);
        this.uriResolvers = Objects.requireNonNull(uriResolvers);
    }

    @Override
    public void validateRequest(final UriTableRequest request) throws StatusRuntimeException {
        final URI uri;
        try {
            uri = URI.create(request.getUri());
        } catch (IllegalArgumentException e) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    String.format("Not a URI '%s'", request.getUri()));
        }
        if (UriHelper.isDeephavenLocal(uri)) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, String.format(
                    "Deephaven local URIs should be resolved via more appropriate APIs '%s'", request.getUri()));
        }
    }

    @Override
    public Table create(final UriTableRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 0);
        validateRequest(request);

        // TODO: check user auth if has access to console
        final boolean resolveSafely = ConsoleServiceGrpcImpl.REMOTE_CONSOLE_DISABLED;
        final URI uri = URI.create(request.getUri());

        Object object;
        try {
            object = resolveSafely ? uriResolvers.resolveSafely(uri) : uriResolvers.resolve(uri);
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
