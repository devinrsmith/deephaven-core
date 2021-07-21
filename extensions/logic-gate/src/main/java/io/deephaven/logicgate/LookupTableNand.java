package io.deephaven.logicgate;

import io.deephaven.qst.column.Column;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.Table;
import io.deephaven.qst.table.TimeTable;

import java.time.Duration;

public class LookupTableNand {

    // @formatter:off
    private static final Table NAND_LOOKUP = ColumnHeader.ofByte("A")
            .header(ColumnHeader.ofByte("B"))
            .header(ColumnHeader.ofByte("Q"))
            .row((byte) 0, (byte)0, (byte)1)
            .row((byte) 0, (byte)1, (byte)1)
            .row((byte) 1, (byte)0, (byte)1)
            .row((byte) 1, (byte)1, (byte)0)
            .build();
    // @formatter:on

    private static final Table ZERO_BIT = NewTable.of(Column.of("Q", (byte) 0));
    private static final Table ONE_BIT = NewTable.of(Column.of("Q", (byte) 1));

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
        public final Table timedBit(Duration duration) {
            return Table.merge(ZERO_BIT, TimeTable.of(duration).view("Q=(byte)(i % 2)")).tail(1);
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
}
