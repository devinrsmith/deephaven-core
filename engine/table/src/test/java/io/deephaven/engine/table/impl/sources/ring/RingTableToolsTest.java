package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.StreamTableTools;
import io.deephaven.engine.table.impl.TstUtils;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.test.junit4.EngineCleanup;
import io.deephaven.time.DateTime;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigInteger;

import static io.deephaven.engine.util.TableTools.byteCol;
import static io.deephaven.engine.util.TableTools.charCol;
import static io.deephaven.engine.util.TableTools.dateTimeCol;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.emptyTable;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.shortCol;
import static org.assertj.core.api.Assertions.assertThat;

public class RingTableToolsTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void staticTableToRing() {
        // TstUtils.assertTableEquals();
        // TstUtils.prevTable();
        // TstUtils.addToTable();

        final Table k = emptyTable(128).view(
                "K_boolean=k%3==0?null:k%3==1",
                "K_byte=(byte)k",
                "K_char=(char)k",
                "K_double=(double)k",
                "K_float=(float)k",
                "K_int=(int)k",
                "K_long=k",
                "K_short=(short)k",
                "K_str=``+k",
                "K_datetime=new DateTime(k)",
                "K_boolean=k%3==0?null:k%3==1");
        for (int capacity = 1; capacity <= 256; ++capacity) {
            final Table tail = k.tail(capacity);
            final Table ring = RingTableTools.of(k, capacity, true);
            checkEquals(tail, ring);
        }
    }

    @Test
    public void streamTableToRing() {
        coprime(1, 93);
        coprime(5, 71);
        coprime(14, 25);
    }

    private static void coprime(int a, int b) {
        if (!BigInteger.valueOf(a).gcd(BigInteger.valueOf(b)).equals(BigInteger.ONE)) {
            throw new IllegalArgumentException("not coprime: " + a + ", " + b);
        }
        cycleTest(a, b, a + 1);
        cycleTest(b, a, b + 1);
    }

    // capacity & appendSize are coprime, and times >= capacity
    // Meant to test the inner state of the ring position among all possible positions.
    private static void cycleTest(int capacity, int appendSize, int times) {
        final ColumnHolder[] holders = {
                byteHolder(appendSize),
                charHolder(appendSize),
                doubleHolder(appendSize),
                floatHolder(appendSize),
                intHolder(appendSize),
                longHolder(appendSize),
                shortHolder(appendSize),
                dateTimeHolder(appendSize)
        };
        final StreamTableHelper sth = new StreamTableHelper(appendSize, holders);
        final Table tail = StreamTableTools.streamToAppendOnlyTable(sth.streamTable).tail(capacity);
        final Table ring = RingTableTools.of(sth.streamTable, capacity, true);
        checkEquals(tail, ring);
        for (int i = 0; i < times; ++i) {
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                sth.addAndNotify(appendSize, holders);
                checkEquals(tail, ring);
                TstUtils.assertTableEquals(TstUtils.prevTable(tail), TstUtils.prevTable(ring));
                TstUtils.assertTableEquals(TstUtils.prevTableColumnSources(tail),
                        TstUtils.prevTableColumnSources(ring));
            });
        }
        assertThat(tail.size()).isEqualTo(capacity);
        assertThat(ring.size()).isEqualTo(capacity);
    }

    private static void checkEquals(Table tail, Table ring) {
        TstUtils.assertTableEquals(tail, ring);

        // dense indices
        TstUtils.assertTableEquals(tail.where("i%3!=0"), ring.where("i%3!=0"));
        TstUtils.assertTableEquals(tail.where("i%9!=0"), ring.where("i%9!=0"));
        TstUtils.assertTableEquals(tail.where("i%27!=0"), ring.where("i%27!=0"));

        // sparse indices
        TstUtils.assertTableEquals(tail.where("i%3==0"), ring.where("i%3==0"));
        TstUtils.assertTableEquals(tail.where("i%9==0"), ring.where("i%9==0"));
        TstUtils.assertTableEquals(tail.where("i%27==0"), ring.where("i%27==0"));
    }

    private static ColumnHolder byteHolder(int appendSize) {
        byte[] col = new byte[appendSize];
        for (int i = 0; i < appendSize; ++i) {
            col[i] = (byte) i;
        }
        return byteCol("X_byte", col);
    }

    private static ColumnHolder charHolder(int appendSize) {
        char[] col = new char[appendSize];
        for (int i = 0; i < appendSize; ++i) {
            col[i] = (char) i;
        }
        return charCol("X_char", col);
    }

    private static ColumnHolder doubleHolder(int appendSize) {
        double[] col = new double[appendSize];
        for (int i = 0; i < appendSize; ++i) {
            col[i] = i;
        }
        return doubleCol("X_double", col);
    }

    private static ColumnHolder floatHolder(int appendSize) {
        float[] col = new float[appendSize];
        for (int i = 0; i < appendSize; ++i) {
            col[i] = i;
        }
        return floatCol("X_float", col);
    }

    private static ColumnHolder intHolder(int appendSize) {
        int[] col = new int[appendSize];
        for (int i = 0; i < appendSize; ++i) {
            col[i] = i;
        }
        return intCol("X_int", col);
    }

    private static ColumnHolder longHolder(int appendSize) {
        long[] col = new long[appendSize];
        for (int i = 0; i < appendSize; ++i) {
            col[i] = i;
        }
        return longCol("X_long", col);
    }

    private static ColumnHolder shortHolder(int appendSize) {
        short[] col = new short[appendSize];
        for (int i = 0; i < appendSize; ++i) {
            col[i] = (short) i;
        }
        return shortCol("X_short", col);
    }

    private static ColumnHolder dateTimeHolder(int appendSize) {
        DateTime[] col = new DateTime[appendSize];
        for (int i = 0; i < appendSize; ++i) {
            col[i] = new DateTime(i);
        }
        return dateTimeCol("X_datetime", col);
    }

    private static class StreamTableHelper {

        private final QueryTable streamTable;
        private int prev;

        public StreamTableHelper(int len, final ColumnHolder... holders) {
            this.streamTable = TstUtils.testRefreshingTable(RowSetFactory.flat(len).toTracking(), holders);
            this.streamTable.setAttribute(Table.STREAM_TABLE_ATTRIBUTE, true);
            this.prev = len;
        }

        public void addAndNotify(int len, final ColumnHolder... holders) {
            final RowSet removed = RowSetFactory.flat(prev);
            final RowSet added = RowSetFactory.flat(len);
            TstUtils.removeRows(streamTable, removed);
            TstUtils.addToTable(streamTable, added, holders);
            streamTable.notifyListeners(added, removed, RowSetFactory.empty());
            prev = len;
        }
    }
}
