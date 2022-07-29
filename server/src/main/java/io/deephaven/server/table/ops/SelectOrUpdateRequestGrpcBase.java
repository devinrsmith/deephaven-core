package io.deephaven.server.table.ops;

import io.deephaven.api.Selectable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.SelectableTable;

import java.util.function.Function;

public abstract class SelectOrUpdateRequestGrpcBase<T extends SelectableTable>
        extends GrpcTableSpecOperation<SelectOrUpdateRequest, T> {

    public SelectOrUpdateRequestGrpcBase(UpdateGraphProcessor ugp,
            final Function<BatchTableRequest.Operation, SelectOrUpdateRequest> getRequest) {
        super(ugp, getRequest, SelectOrUpdateRequest::getResultId, SelectOrUpdateRequest::getSourceId);
    }

    @Override
    void validateSecurity(T spec, TableCreator<Table> creator) {
        validateColumns(spec, creator);
    }

    <Builder extends SelectableTable.Builder<T, Builder>> T build(Builder builder, SelectOrUpdateRequest request) {
        builder.parent(GrpcExportTable.of(0));
        for (String columnSpec : request.getColumnSpecsList()) {
            builder.addColumns(Selectable.parse(columnSpec));
        }
        return builder.build();
    }
}
