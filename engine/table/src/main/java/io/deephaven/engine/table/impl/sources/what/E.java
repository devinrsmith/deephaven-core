package io.deephaven.engine.table.impl.sources.what;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSet.RangeIterator;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.stream.StreamToTableAdapter;

public class E {

    public static Table ok(Table table) {
        final TableUpdatePublisher publisher = new TableUpdatePublisher();
        final StreamToTableAdapter adapter = new StreamToTableAdapter(TableUpdatePublisher.definition(), publisher, UpdateGraphProcessor.DEFAULT, "todo");
        final Table theTable = adapter.table();
        final Bridge listener = new Bridge("todo", table, (QueryTable) theTable, publisher);
        table.addUpdateListener(listener);
        return theTable;
    }

    public static Table decorate(Table t) {
        return t.view(
                "AddedSize=TableUpdate.added().size()",
                "AddedRangeCount=io.deephaven.engine.table.impl.sources.what.E.rangeCount(TableUpdate.added())",
                "AddedFirstRowKey=TableUpdate.added().firstRowKey()",
                "AddedLastRowKey=TableUpdate.added().lastRowKey()",
                "AddedIsFlat=TableUpdate.added().isFlat()",
                "AddedIsContiguous=TableUpdate.added().isContiguous()",
                "RemovedSize=TableUpdate.removed().size()",
                "RemovedRangeCount=io.deephaven.engine.table.impl.sources.what.E.rangeCount(TableUpdate.removed())",
                "RemovedFirstRowKey=TableUpdate.removed().firstRowKey()",
                "RemovedLastRowKey=TableUpdate.removed().lastRowKey()",
                "RemovedIsFlat=TableUpdate.removed().isFlat()",
                "RemovedIsContiguous=TableUpdate.removed().isContiguous()",
                "ModifiedSize=TableUpdate.modified().size()",
                "ModifiedRangeCount=io.deephaven.engine.table.impl.sources.what.E.rangeCount(TableUpdate.modified())",
                "ModifiedFirstRowKey=TableUpdate.modified().firstRowKey()",
                "ModifiedLastRowKey=TableUpdate.modified().lastRowKey()",
                "ModifiedIsFlat=TableUpdate.modified().isFlat()",
                "ModifiedIsContiguous=TableUpdate.modified().isContiguous()",
                "ShiftSize=TableUpdate.shifted().size()",
                "ShiftKeySize=TableUpdate.shifted().getEffectiveSize()",
                "ModifiedColumnSetSize=TableUpdate.modifiedColumnSet().size()");
    }

    public static int rangeCount(RowSet rowSet) {
        int rangeCount = 0;
        try (final RangeIterator rangeIterator = rowSet.rangeIterator()) {
            while (rangeIterator.hasNext()) {
                rangeIterator.next();
                ++rangeCount;
            }
        }
        return rangeCount;
    }
}
