package io.deephaven.server.table.ops;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.RingTableRequest;
import io.deephaven.server.grpc.Common;
import io.deephaven.server.grpc.GrpcErrorHelper;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public final class RingTableGrpcImpl extends GrpcTableOperation<RingTableRequest> {

    @Inject
    public RingTableGrpcImpl(TableServiceContextualAuthWiring auth) {
        super(
                auth::checkPermissionRingTable,
                Operation::getRing,
                RingTableRequest::getResultId,
                RingTableRequest::getSourceId);
    }

    @Override
    public void validateRequest(RingTableRequest request) throws StatusRuntimeException {
        GrpcErrorHelper.checkHasField(request, RingTableRequest.SOURCE_ID_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(request);
        Common.validate(request.getSourceId());
    }

    @Override
    public Table create(RingTableRequest request, List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        final Table parent = sourceTables.get(0).get();
        return RingTableTools.of(parent, request.getSize(), request.getInitialize());
    }
}
