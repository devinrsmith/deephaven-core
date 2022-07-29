/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.LazyUpdateTable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class LazyUpdateGrpcImpl extends SelectOrUpdateRequestGrpcBase<LazyUpdateTable> {
    @Inject
    public LazyUpdateGrpcImpl(UpdateGraphProcessor ugp) {
        super(ugp, BatchTableRequest.Operation::getLazyUpdate);
    }

    @Override
    LazyUpdateTable createTableSpec(SelectOrUpdateRequest request) {
        return build(LazyUpdateTable.builder(), request);
    }

    @Override
    LockType lockType(LazyUpdateTable spec, TableCreator<Table> creator) {
        final Table parentTable = spec.parent().logic().create(creator);
        return parentTable.isRefreshing() ? LockType.SHARED : LockType.NONE;
    }
}
