package io.deephaven.logicgate;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.config.MutableInputTable;
import io.deephaven.db.v2.select.SourceColumn;
import io.deephaven.db.v2.utils.KeyedArrayBackedMutableTable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Objects;

public class LookupTableNand {

    private static final Table NAND_LOOKUP =
        TableTools.newTable(TableTools.byteCol("A", (byte) 0, (byte) 0, (byte) 1, (byte) 1),
            TableTools.byteCol("B", (byte) 0, (byte) 1, (byte) 0, (byte) 1),
            TableTools.byteCol("Q", (byte) 1, (byte) 1, (byte) 1, (byte) 0));

    private static final Table ZERO_BIT = TableTools.newTable(TableTools.byteCol("Q", (byte) 0));
    private static final Table ONE_BIT = TableTools.newTable(TableTools.byteCol("Q", (byte) 1));

    private static final SourceColumn Q_SOURCE_COLUMN = new SourceColumn("Q");
    private static final MatchPair A = new MatchPair("A", "A");
    private static final MatchPair B = new MatchPair("B", "B");
    private static final MatchPair[] ADD_B = {B};
    private static final MatchPair A_EQ_Q = new MatchPair("A", "Q");
    private static final MatchPair B_EQ_Q = new MatchPair("B", "Q");

    public static LogicGateBuilder create() {
        return ImmutableNandBasedBuilder.builder().bitBuilder(Builder.INSTANCE)
            .nandBuilder(Builder.INSTANCE).build();
    }

    private enum Builder implements BitBuilder, NandBuilder {
        INSTANCE;

        @Override
        public final Table zero() {
            return ZERO_BIT;
        }

        @Override
        public final Table one() {
            return ONE_BIT;
        }

        @Override
        public final SettableBit settable() {
            return new MySettable(KeyedArrayBackedMutableTable.make(ZERO_BIT));
        }

        @Override
        public final Table timedBit(Duration duration) {
            return TableTools.merge(ZERO_BIT, TableTools.timeTable(duration.toNanos()).view("Q=(byte)(i % 2)")).tail(1);
        }

        @Override
        public final Table nand(Table a, Table b) {
            /*
             * Table left = a.renameColumns(A_EQ_Q); Table right = b.renameColumns(B_EQ_Q); return
             * NAND_LOOKUP .whereIn(left.naturalJoin(right, MatchPair.ZERO_LENGTH_MATCH_PAIR_ARRAY,
             * ADD_B), A, B) .view(Q_SOURCE_COLUMN);
             */

            return NAND_LOOKUP.whereIn(a.view("A=Q").naturalJoin(b.view("B=Q"), ""), "A", "B")
                .view("Q");
        }
    }

    private static class MySettable implements SettableBit {
        private final KeyedArrayBackedMutableTable table;

        MySettable(KeyedArrayBackedMutableTable table) {
            this.table = Objects.requireNonNull(table);
        }

        @Override
        public Table bit() {
            return table;
        }

        @Override
        public void clear() {
            try {
                ((MutableInputTable) table.getAttribute(Table.INPUT_TABLE_ATTRIBUTE)).add(ZERO_BIT);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void set() {
            try {
                ((MutableInputTable) table.getAttribute(Table.INPUT_TABLE_ATTRIBUTE)).add(ONE_BIT);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
