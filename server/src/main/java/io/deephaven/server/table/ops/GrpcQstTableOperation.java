package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.TableCreatorForImplementation;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public abstract class GrpcQstTableOperation<T, S extends TableSpec> extends GrpcTableOperation<T> {

    public GrpcQstTableOperation(Function<Operation, T> getRequest, Function<T, Ticket> getTicket,
            MultiDependencyFunction<T> getDependencies) {
        super(getRequest, getTicket, getDependencies);
    }

    public GrpcQstTableOperation(Function<Operation, T> getRequest, Function<T, Ticket> getTicket,
            Function<T, TableReference> getDependency) {
        super(getRequest, getTicket, getDependency);
    }

    public GrpcQstTableOperation(Function<Operation, T> getRequest, Function<T, Ticket> getTicket) {
        super(getRequest, getTicket);
    }

    @Override
    public final void validateRequest(T request) throws StatusRuntimeException {
        final S spec;
        try {
            spec = createTableSpec(request);
        } catch (IllegalArgumentException e) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, e.getMessage());
        }
        validateSecurity(spec);
    }

    @Override
    public final Table create(T request, List<ExportObject<Table>> sourceTables) {
        final S tableSpec = createTableSpec(request);
        final TableCreatorGrpcImpl creator = new TableCreatorGrpcImpl(sourceTables);
        // todo: UpdateGraphProcessor for certain ops
        return creator.create(tableSpec);
    }

    abstract S createTableSpec(T request);

    abstract void validateSecurity(S spec);

    private static final class TableCreatorGrpcImpl extends TableCreatorForImplementation<Table, GrpcExportTable> {
        private final List<ExportObject<Table>> sourceTables;

        public TableCreatorGrpcImpl(List<ExportObject<Table>> sourceTables) {
            super(GrpcExportTable.class);
            this.sourceTables = Objects.requireNonNull(sourceTables);
        }

        @Override
        public Table ofImplementation(GrpcExportTable implementation) {
            return sourceTables.get(implementation.index()).get();
        }

        public Table create(TableSpec table) {
            return table.logic().create(this);
        }
    }
}
