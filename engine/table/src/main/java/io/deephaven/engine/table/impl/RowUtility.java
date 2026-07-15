package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.sources.RowKeyColumnSource;
import io.deephaven.engine.table.impl.sources.RowPositionColumnSource;

import java.util.Map;

public final class RowUtility {

    public static QueryTable withRowPosition(final QueryTable table, final String columnName) {
        if (table.hasColumns(columnName)) {
            // this limitation does make the contract of the method easier to understand, and possibly simplifies the
            // rest of the code in this method.
            throw new IllegalArgumentException();
        }
        final ColumnSource<?> cs = table.isFlat()
                ? RowKeyColumnSource.INSTANCE
                : new RowPositionColumnSource(table.getRowSet());
        return impl(table, columnName, "Row Position", cs);
    }

    public static QueryTable withRowKey(final QueryTable table, final String columnName) {
        if (table.hasColumns(columnName)) {
            // this limitation does make the contract of the method easier to understand, and possibly simplifies the
            // rest of the code in this method.
            throw new IllegalArgumentException();
        }
        return impl(table, columnName, "Row Key", RowKeyColumnSource.INSTANCE);
    }

    private static QueryTable impl(QueryTable table, String name, String description, ColumnSource<?> columnSource) {
        final QueryTable result = table.withAdditionalColumns(Map.of(name, columnSource));
        // TODO: no easy way to add
        // result.withColumnDescription(name, String.format("A row key column applied to Table %s", System.identityHashCode(table)));
        {
            // io.deephaven.engine.table.impl.QueryTable.lazyUpdate
            // propagateFlatness(result);
            // copyAttributes(result, BaseTable.CopyAttributeOperation.UpdateView);
            // copySortableColumns(result, processedColumns);
            // maybeCopyColumnDescriptions(result, processedColumns);
            // Possibly, we should copy more attributes?

            // are there any attributes we should *not* copy?
            table.copyAttributes(result, x -> true);
            // table.propagateFlatness(result);
            // it seems like there is special handling for UpdateView; we may be better off lying and using something like DropColumn, View, or Coalesce?
            // table.copyAttributes(result, BaseTable.CopyAttributeOperation.UpdateView);
            // table.copyAttributes(result, BaseTable.CopyAttributeOperation.Coalesce);
            // table.copySortableColumns(result, x -> true);
            // table.maybeCopyColumnDescriptions(result);
        }
        // result.withAttributes(table.getAttributes());
        table.addUpdateListener(new PropagateForStatelessColumnSource(table, result));
        return result;
    }

    private static class PropagateForStatelessColumnSource extends BaseTable.ListenerImpl {
        private final ModifiedColumnSet mcs;
        private final ModifiedColumnSet.Transformer transformer;

        PropagateForStatelessColumnSource(final QueryTable parent, final QueryTable result) {
            super(PropagateForStatelessColumnSource.class.getName(), parent, result);
            mcs = result.getModifiedColumnSetForUpdates();
            // The row key / row position column is never relayed as modified since it is constant; using the parent
            // column names which we've already verified does not contain the column.
            transformer = parent.newModifiedColumnSetTransformer(result, parent.getDefinition().getColumnNamesArray());
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            transformer.clearAndTransform(upstream.modifiedColumnSet(), mcs);
            getDependent().notifyListeners(TableUpdateImpl.copy(upstream, mcs));
        }
    }

    private RowUtility() {}
}
