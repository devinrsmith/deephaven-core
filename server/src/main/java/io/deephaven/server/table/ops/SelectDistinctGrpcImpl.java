/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.ops;

import io.deephaven.api.Selectable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectDistinctRequest;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.SelectDistinctTable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SelectDistinctGrpcImpl extends GrpcTableSpecOperation<SelectDistinctRequest, SelectDistinctTable> {
    @Inject
    public SelectDistinctGrpcImpl(UpdateGraphProcessor ugp) {
        super(ugp, BatchTableRequest.Operation::getSelectDistinct, SelectDistinctRequest::getResultId,
                SelectDistinctRequest::getSourceId);
    }

    @Override
    SelectDistinctTable createTableSpec(SelectDistinctRequest request) {
        SelectDistinctTable.Builder builder = SelectDistinctTable.builder().parent(GrpcExportTable.of(0));
        for (String columnName : request.getColumnNamesList()) {
            builder.addColumns(Selectable.parse(columnName));
        }
        return builder.build();
    }

    @Override
    void validateSecurity(SelectDistinctTable spec, TableCreator<Table> creator) {
        validateColumns(spec, creator);
    }

    @Override
    LockType lockType(SelectDistinctTable spec, TableCreator<Table> creator) {
        return LockType.NONE;
    }
}
