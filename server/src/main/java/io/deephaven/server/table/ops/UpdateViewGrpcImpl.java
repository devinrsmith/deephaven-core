/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.UpdateViewTable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class UpdateViewGrpcImpl extends SelectOrUpdateRequestGrpcBase<UpdateViewTable> {
    @Inject
    public UpdateViewGrpcImpl(UpdateGraphProcessor ugp) {
        super(ugp, BatchTableRequest.Operation::getUpdateView);
    }

    @Override
    UpdateViewTable createTableSpec(SelectOrUpdateRequest request) {
        return build(UpdateViewTable.builder(), request);
    }

    @Override
    LockType lockType(UpdateViewTable spec, TableCreator<Table> creator) {
        return LockType.NONE;
    }
}
