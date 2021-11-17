package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.session.SessionState.ExportObject;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.FetchTableRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.util.auth.AuthContext;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import java.util.List;

public class FetchTableGrpcImpl extends GrpcTableOperation<FetchTableRequest> {
    @Inject()
    protected FetchTableGrpcImpl() {
        super(BatchTableRequest.Operation::getFetchTable, FetchTableRequest::getResultId,
                FetchTableRequest::getSourceId);
    }

    @Override
    public void validateRequest(AuthContext auth, FetchTableRequest request) throws StatusRuntimeException {
        if (request.getSourceId().hasTicket()) {
            final Ticket ticket = request.getSourceId().getTicket();
            // todo
        }
    }

    @Override
    public Table create(AuthContext auth, FetchTableRequest request, List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 1);
        return sourceTables.get(0).get();
    }
}
