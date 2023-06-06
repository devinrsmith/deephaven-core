package io.deephaven.app;

import com.sun.management.GarbageCollectionNotificationInfo;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.qst.array.LongArray.Builder;

public enum MyId implements LongMapper<GarbageCollectionNotificationInfo> {
    INSTANCE;

    private final ColumnDefinition<Long> columnDefinition = ColumnDefinition.ofLong("Id");

    @Override
    public ColumnDefinition<Long> definition() {
        return columnDefinition;
    }

    @Override
    public void add(Builder builder, GarbageCollectionNotificationInfo notificationInfo) {
        builder.add(notificationInfo.getGcInfo().getId());
    }
}
