package io.deephaven.qst;

import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ImplementationTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;

import java.util.Objects;

public abstract class TableCreatorForImplementation<T, I extends ImplementationTable> implements TableCreator<T> {

    private final Class<I> expectedClass;

    public TableCreatorForImplementation(Class<I> expectedClass) {
        this.expectedClass = Objects.requireNonNull(expectedClass);
    }

    public abstract T ofImplementation(I implementation);

    @Override
    public T of(ImplementationTable implementationTable) {
        if (!expectedClass.isInstance(implementationTable)) {
            throw new UnsupportedOperationException(String.format("Should only be resolving '%s'", expectedClass));
        }
        return ofImplementation(expectedClass.cast(implementationTable));
    }

    @Override
    public T of(NewTable newTable) {
        throw new UnsupportedOperationException(String.format("Should only be resolving '%s'", expectedClass));
    }

    @Override
    public T of(EmptyTable emptyTable) {
        throw new UnsupportedOperationException(String.format("Should only be resolving '%s'", expectedClass));
    }

    @Override
    public T of(TimeTable timeTable) {
        throw new UnsupportedOperationException(String.format("Should only be resolving '%s'", expectedClass));
    }

    @Override
    public T of(TicketTable ticketTable) {
        throw new UnsupportedOperationException(String.format("Should only be resolving '%s'", expectedClass));
    }

    @Override
    public T of(InputTable inputTable) {
        throw new UnsupportedOperationException(String.format("Should only be resolving '%s'", expectedClass));
    }

    @Override
    public T merge(Iterable<T> ts) {
        throw new UnsupportedOperationException(String.format("Should only be resolving '%s'", expectedClass));
    }
}
