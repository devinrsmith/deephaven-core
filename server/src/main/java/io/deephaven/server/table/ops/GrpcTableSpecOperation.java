package io.deephaven.server.table.ops;

import com.google.rpc.Code;
import io.deephaven.api.Strings;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.TableCreatorImpl;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.TableCreatorForImplementation;
import io.deephaven.qst.table.SelectableTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.server.session.SessionState.ExportObject;
import io.deephaven.server.table.validation.ColumnExpressionValidator;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

public abstract class GrpcTableSpecOperation<T, S extends TableSpec> extends GrpcTableOperation<T> {

    private final UpdateGraphProcessor ugp;

    public GrpcTableSpecOperation(UpdateGraphProcessor ugp, Function<Operation, T> getRequest,
            Function<T, Ticket> getTicket,
            MultiDependencyFunction<T> getDependencies) {
        super(getRequest, getTicket, getDependencies);
        this.ugp = Objects.requireNonNull(ugp);
    }

    public GrpcTableSpecOperation(UpdateGraphProcessor ugp, Function<Operation, T> getRequest,
            Function<T, Ticket> getTicket,
            Function<T, TableReference> getDependency) {
        super(getRequest, getTicket, getDependency);
        this.ugp = Objects.requireNonNull(ugp);
    }

    public GrpcTableSpecOperation(UpdateGraphProcessor ugp, Function<Operation, T> getRequest,
            Function<T, Ticket> getTicket) {
        super(getRequest, getTicket);
        this.ugp = Objects.requireNonNull(ugp);
    }

    @Override
    public final void validateRequest(T request) throws StatusRuntimeException {
        try {
            createTableSpec(request);
        } catch (IllegalArgumentException e) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, e.getMessage());
        }
    }

    @Override
    public final Table create(T request, List<ExportObject<Table>> sourceTables) {
        final S tableSpec = createTableSpec(request);
        final TableCreatorGrpcImpl creator = new TableCreatorGrpcImpl(sourceTables);
        // Note: all the following logic could be extracted and made gRPC-agnostic
        validateSecurity(tableSpec, creator);
        final Lock lock = lock(tableSpec, creator).orElse(null);
        if (lock != null) {
            lock.lock();
        }
        try {
            return creator.create(tableSpec);
        } finally {
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    enum LockType {
        NONE, SHARED, EXCLUSIVE
    }

    abstract S createTableSpec(T request);

    abstract void validateSecurity(S spec, TableCreator<Table> creator);

    abstract LockType lockType(S spec, TableCreator<Table> creator);

    private Optional<Lock> lock(S spec, TableCreator<Table> creator) {
        LockType lockType = lockType(spec, creator);
        switch (lockType) {
            case NONE:
                return Optional.empty();
            case SHARED:
                return Optional.of(ugp.sharedLock());
            case EXCLUSIVE:
                return Optional.of(ugp.exclusiveLock());
            default:
                throw new IllegalStateException("Unexpected lock type: " + lockType);
        }
    }

    private static final class TableCreatorGrpcImpl extends TableCreatorForImplementation<Table, GrpcExportTable> {
        private final List<ExportObject<Table>> sourceTables;

        public TableCreatorGrpcImpl(List<ExportObject<Table>> sourceTables) {
            super(TableCreatorImpl.INSTANCE, GrpcExportTable.class);
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

    static void validateColumns(SelectableTable selectableTable, TableCreator<Table> creator) {
        final SelectColumn[] selectColumns = SelectColumn.from(selectableTable.columns());
        final String[] stringExpressions = selectableTable.columns().stream().map(Strings::of).toArray(String[]::new);
        final Table parentTable = selectableTable.parent().logic().create(creator);
        ColumnExpressionValidator.validateColumnExpressions(selectColumns, stringExpressions, parentTable);
    }
}
