/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.EmptyTable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class EmptyTableGrpcImpl extends GrpcTableSpecOperation<EmptyTableRequest, EmptyTable> {

    @Inject
    public EmptyTableGrpcImpl(UpdateGraphProcessor ugp) {
        super(ugp, BatchTableRequest.Operation::getEmptyTable, EmptyTableRequest::getResultId);
    }

    @Override
    EmptyTable createTableSpec(EmptyTableRequest request) {
        return EmptyTable.of(request.getSize());
    }

    @Override
    void validateSecurity(EmptyTable spec, TableCreator<Table> creator) {
        // EmptyTable secure-by-default
    }

    @Override
    LockType lockType(EmptyTable spec, TableCreator<Table> creator) {
        return LockType.NONE;
    }
}
