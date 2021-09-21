package io.deephaven.db.v2.utils;

import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.BooleanArray;
import io.deephaven.qst.array.ByteArray;
import io.deephaven.qst.array.CharArray;
import io.deephaven.qst.array.DoubleArray;
import io.deephaven.qst.array.FloatArray;
import io.deephaven.qst.array.GenericArray;
import io.deephaven.qst.array.IntArray;
import io.deephaven.qst.array.LongArray;
import io.deephaven.qst.array.PrimitiveArray;
import io.deephaven.qst.array.ShortArray;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class AppendOnlyDiskBackedMutableTable  {

    public static AppendOnlyDiskBackedMutableTable make(TableHeader header) {
        return null;
    }

    public static AppendOnlyDiskBackedMutableTable make(TableHeader header, Path path) {
        return null;
    }

    public static void testSequential() {
        final int L = 1000 * 1000;
        final int[] ints = new int[L];
        final long[] longs = new long[L];
        long now = System.currentTimeMillis();
        for (int i = 0; i < L; ++i) {
            ints[i] = (int)(now - i);
        }
        for (int i = 0; i < L; i++) {
            longs[i] = now + i;
        }
        test(NewTable.of(Column.ofInt("Int", ints), Column.ofLong("Long", longs)), Paths.get("/tmp/my-db-seq"));
    }

    public static void testRandom() {
        final int L = 1000 * 1000;
        final int[] ints = new int[L];
        final long[] longs = new long[L];
        final Random r = new Random();
        for (int i = 0; i < L; ++i) {
            ints[i] = r.nextInt();
        }
        for (int i = 0; i < L; i++) {
            longs[i] = r.nextLong();
        }
        test(NewTable.of(Column.ofInt("Int", ints), Column.ofLong("Long", longs)), Paths.get("/tmp/my-db-rand"));
    }

    public static void test(NewTable table, Path path) {
        try (final Env<ByteBuffer> env = Env.create()
                .setMaxDbs(table.numColumns())
                .setMapSize(64_000_000)
                .open(path.toFile())) {
            final Map<String, Dbi<ByteBuffer>> dbs = new HashMap<>(table.numColumns());
            for (Column<?> column : table) {
                final Dbi<ByteBuffer> dbi = env.openDbi(column.name(), DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
                dbs.put(column.name(), dbi);
            }
            try (final Txn<ByteBuffer> txn = env.txnWrite()) {
                for (Column<?> column : table) {
                    final Dbi<ByteBuffer> dbi = dbs.get(column.name());
                    column.array().walk(new SequentialPut(txn, dbi, 0));
                }
                txn.commit();
            }
        }
    }

    private static class SequentialPut implements Array.Visitor, PrimitiveArray.Visitor {
        private final Txn<ByteBuffer> txn;
        private final Dbi<ByteBuffer> dbi;
        private final long start;

        public SequentialPut(Txn<ByteBuffer> txn, Dbi<ByteBuffer> dbi, long start) {
            this.txn = Objects.requireNonNull(txn);
            this.dbi = Objects.requireNonNull(dbi);
            this.start = start;
        }

        @Override
        public void visit(PrimitiveArray<?> primitive) {
            primitive.walk((PrimitiveArray.Visitor) this);
        }

        @Override
        public void visit(GenericArray<?> generic) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void visit(ByteArray byteArray) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void visit(BooleanArray booleanArray) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void visit(CharArray charArray) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void visit(ShortArray shortArray) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void visit(IntArray intArray) {
            final ByteBuffer keyB = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder());
            final ByteBuffer valueB = ByteBuffer.allocateDirect(4);
            int offset = 0;
            for (int value : intArray.values()) {
                keyB.clear();
                keyB.putLong(start + offset);
                keyB.flip();

                valueB.clear();
                valueB.putInt(value);
                valueB.flip();

                dbi.put(txn, keyB, valueB, PutFlags.MDB_APPEND, PutFlags.MDB_NOOVERWRITE);
                ++offset;
            }
        }

        @Override
        public void visit(LongArray longArray) {
            final ByteBuffer keyB = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder());
            final ByteBuffer valueB = ByteBuffer.allocateDirect(8);
            int offset = 0;
            for (long value : longArray.values()) {
                keyB.clear();
                keyB.putLong(start + offset);
                keyB.flip();

                valueB.clear();
                valueB.putLong(value);
                valueB.flip();

                dbi.put(txn, keyB, valueB, PutFlags.MDB_APPEND, PutFlags.MDB_NOOVERWRITE);
                ++offset;
            }
        }

        @Override
        public void visit(FloatArray floatArray) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void visit(DoubleArray doubleArray) {
            throw new UnsupportedOperationException("todo");
        }
    }
}
