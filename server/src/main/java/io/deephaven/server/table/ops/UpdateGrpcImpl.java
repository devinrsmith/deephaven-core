/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.UpdateTable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class UpdateGrpcImpl extends SelectOrUpdateRequestGrpcBase<UpdateTable> {
    @Inject
    public UpdateGrpcImpl(UpdateGraphProcessor ugp) {
        super(ugp, BatchTableRequest.Operation::getUpdate);
    }

    @Override
    UpdateTable createTableSpec(SelectOrUpdateRequest request) {
        return build(UpdateTable.builder(), request);
    }

    @Override
    LockType lockType(UpdateTable spec, TableCreator<Table> creator) {
        final Table parentTable = spec.parent().logic().create(creator);
        return parentTable.isRefreshing() ? LockType.SHARED : LockType.NONE;
    }
}
