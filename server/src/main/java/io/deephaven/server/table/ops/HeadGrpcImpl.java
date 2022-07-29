/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.HeadOrTailRequest;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.HeadTable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class HeadGrpcImpl extends GrpcTableSpecOperation<HeadOrTailRequest, HeadTable> {

    @Inject
    public HeadGrpcImpl(final UpdateGraphProcessor ugp) {
        super(ugp, BatchTableRequest.Operation::getHead, HeadOrTailRequest::getResultId,
                HeadOrTailRequest::getSourceId);
    }

    @Override
    HeadTable createTableSpec(HeadOrTailRequest request) {
        return HeadTable.of(GrpcExportTable.of(0), request.getNumRows());
    }

    @Override
    void validateSecurity(HeadTable spec, TableCreator<Table> creator) {
        // HeadTable secure-by-default
    }

    @Override
    LockType lockType(HeadTable spec, TableCreator<Table> creator) {
        return LockType.NONE;
    }
}
