package io.deephaven.grpc_api.table.ops;

import com.google.rpc.Code;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.grpc_api.session.SessionState.ExportObject;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.util.auth.AuthContext;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

@Singleton
public class EmptyTableGrpcImpl extends GrpcTableOperation<EmptyTableRequest> {

    @Inject()
    public EmptyTableGrpcImpl() {
        super(BatchTableRequest.Operation::getEmptyTable, EmptyTableRequest::getResultId);
    }

    @Override
    public void validateRequest(AuthContext auth, final EmptyTableRequest request) throws StatusRuntimeException {
        if (request.getSize() < 0) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Size must be greater than zero");
        }
    }

    @Override
    public Table create(AuthContext auth, final EmptyTableRequest request,
            final List<ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 0);

        return TableTools.emptyTable(request.getSize());
    }
}
