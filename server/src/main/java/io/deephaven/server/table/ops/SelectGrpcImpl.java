/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.SelectTable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SelectGrpcImpl extends SelectOrUpdateRequestGrpcBase<SelectTable> {
    @Inject
    public SelectGrpcImpl(UpdateGraphProcessor ugp) {
        super(ugp, BatchTableRequest.Operation::getSelect);
    }

    @Override
    SelectTable createTableSpec(SelectOrUpdateRequest request) {
        return build(SelectTable.builder(), request);
    }

    @Override
    LockType lockType(SelectTable spec, TableCreator<Table> creator) {
        final Table parentTable = spec.parent().logic().create(creator);
        return parentTable.isRefreshing() ? LockType.SHARED : LockType.NONE;
    }
}
