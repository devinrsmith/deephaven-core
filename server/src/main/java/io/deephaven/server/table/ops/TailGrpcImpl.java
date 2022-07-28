/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.TailTable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class TailGrpcImpl extends GrpcQstTableOperation<HeadOrTailRequest, TailTable> {

    @Inject
    public TailGrpcImpl(final UpdateGraphProcessor ugp) {
        super(ugp, BatchTableRequest.Operation::getTail, HeadOrTailRequest::getResultId,
                HeadOrTailRequest::getSourceId);
    }

    @Override
    TailTable createTableSpec(HeadOrTailRequest request) {
        return TailTable.of(GrpcExportTable.of(0), request.getNumRows());
    }

    @Override
    void validateSecurity(TailTable spec, TableCreator<Table> creator) {
        // HeadTable secure-by-default
    }

    @Override
    LockType lockType(TailTable spec, TableCreator<Table> creator) {
        return LockType.NONE;
    }
}
