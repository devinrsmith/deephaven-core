/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.ViewTable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ViewGrpcImpl extends SelectOrUpdateRequestGrpcBase<ViewTable> {
    @Inject
    public ViewGrpcImpl(UpdateGraphProcessor ugp) {
        super(ugp, BatchTableRequest.Operation::getView);
    }

    @Override
    ViewTable createTableSpec(SelectOrUpdateRequest request) {
        return build(ViewTable.builder(), request);
    }

    @Override
    LockType lockType(ViewTable spec, TableCreator<Table> creator) {
        return LockType.NONE;
    }
}
