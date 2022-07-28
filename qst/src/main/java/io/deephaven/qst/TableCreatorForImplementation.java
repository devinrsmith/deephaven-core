package io.deephaven.qst;

import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.ImplementationTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;

import java.util.Objects;

public abstract class TableCreatorForImplementation<T, I extends ImplementationTable> implements TableCreator<T> {

    private final TableCreator<T> delegate;
    private final Class<I> expectedClass;

    public TableCreatorForImplementation(TableCreator<T> delegate, Class<I> expectedClass) {
        this.delegate = Objects.requireNonNull(delegate);
        this.expectedClass = Objects.requireNonNull(expectedClass);
    }

    public abstract T ofImplementation(I implementation);

    @Override
    public T of(ImplementationTable implementationTable) {
        if (expectedClass.isInstance(implementationTable)) {
            return ofImplementation(expectedClass.cast(implementationTable));
        }
        return delegate.of(implementationTable);
    }

    @Override
    public T of(NewTable newTable) {
        return delegate.of(newTable);
    }

    @Override
    public T of(EmptyTable emptyTable) {
        return delegate.of(emptyTable);
    }

    @Override
    public T of(TimeTable timeTable) {
        return delegate.of(timeTable);
    }

    @Override
    public T of(TicketTable ticketTable) {
        return delegate.of(ticketTable);
    }

    @Override
    public T of(InputTable inputTable) {
        return delegate.of(inputTable);
    }

    @Override
    public T merge(Iterable<T> ts) {
        return delegate.merge(ts);
    }
}
