package io.deephaven.engine.table.impl.sources.what;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;

import java.util.Objects;

// bridges the listener / publisher models
class Bridge extends BaseTable.ListenerImpl {
    private final TableUpdatePublisher publisher;
    public Bridge(String description, Table parent, BaseTable<?> dependent, TableUpdatePublisher publisher) {
        super(description, parent, dependent);
        this.publisher = Objects.requireNonNull(publisher);
    }

    @Override
    public void onUpdate(TableUpdate upstream) {
        // todo: refs?
        // todo decrement when falls out of view
        publisher.add(upstream.acquire());
    }

    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        publisher.acceptFailure(originalException);
    }
}
