/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.immutable;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.LongRangeAbortableConsumer;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.CursorIterable.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.KeyRange;
import org.lmdbjava.PutFlags;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Txn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * Simple array source for Immutable Int.
 * <p>
 * The ImmutableC-harArraySource is replicated to all other types with
 * io.deephaven.db.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ImmutableIntArraySourceDiskLmdb extends AbstractColumnSource<Integer> implements ImmutableColumnSourceGetDefaults.ForInt {

    public static void write(Path path, int amount) throws IOException {
        final int numBytes = Math.multiplyExact(amount, 4);
        final Env<ByteBuffer> env = Env.create().setMapSize(10000000000L).open(path.toFile());
        final Dbi<ByteBuffer> db = env.openDbi((String) null, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
        final ByteBuffer key = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
        final ByteBuffer value = ByteBuffer.allocateDirect(4);
        try (final Txn<ByteBuffer> txn = env.txnWrite()) {
            for (int i = 0; i < amount; ++i) {
                key.clear();
                key.putInt(i);
                key.flip();

                value.clear();
                value.putInt(i);
                value.flip();

                db.put(txn, key, value, PutFlags.MDB_APPEND);
            }
            txn.commit();
        }
    }

    public static Table tableSum(Path path, int amount) {
        final int numBytes = Math.multiplyExact(amount, 4);

        try (final Env<ByteBuffer> env = Env.create().setMapSize(10000000000L).open(path.toFile(), EnvFlags.MDB_RDONLY_ENV)) {
            final Dbi<ByteBuffer> db = env.openDbi((String) null, DbiFlags.MDB_INTEGERKEY);

            try (final Txn<ByteBuffer> txn = env.txnRead()) {
                final ImmutableIntArraySourceDiskLmdb source = new ImmutableIntArraySourceDiskLmdb(amount, db, txn);
                final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>();
                sources.put("X", source);
                final Table results = new QueryTable(Index.FACTORY.getFlatIndex(amount), sources).sumBy();
                txn.commit();
                return results;
            }
        }
    }

    public static long sumDirect(Path path) {
        try (final Env<ByteBuffer> env = Env.create().setMapSize(10000000000L).open(path.toFile(), EnvFlags.MDB_RDONLY_ENV)) {
            final Dbi<ByteBuffer> db = env.openDbi((String) null, DbiFlags.MDB_INTEGERKEY);
            try (final Txn<ByteBuffer> txn = env.txnRead()) {
                long sum = 0;
                for (KeyVal<ByteBuffer> kv : db.iterate(txn)) {
                    sum += kv.val().getInt();
                }
                txn.commit();
                return sum;
            }
        }
    }

    private final int size;
    private final Dbi<ByteBuffer> db;
    private final Txn<ByteBuffer> txn;

    private final ByteBuffer key;

    private ImmutableIntArraySourceDiskLmdb(int size, Dbi<ByteBuffer> db, Txn<ByteBuffer> txn) {
        super(int.class);
        this.size = size;
        this.db = Objects.requireNonNull(db);
        this.txn = Objects.requireNonNull(txn);
        key = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
    }

    @Override
    public int getInt(long index) {
        if (index < 0 || index >= size) {
            return NULL_INT;
        }
        key.clear();
        key.putInt((int)index);
        key.flip();
        return db.get(txn, key).getInt();
    }

    @Override
    public Integer get(long index) {
        if (index < 0 || index >= size) {
            return null;
        }
        key.clear();
        key.putInt((int)index);
        key.flip();
        return TypeUtils.box(db.get(txn, key).getInt());
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull OrderedKeys orderedKeys) {
        final ByteBuffer start = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
        final ByteBuffer stop = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
        final WritableIntChunk<? super Values> intChunk = destination.asWritableIntChunk();
        final MutableInt destPos = new MutableInt(0);
        orderedKeys.forAllLongRanges((first, last) -> {
            start.clear();
            start.putInt((int)first);
            start.flip();

            stop.clear();
            stop.putInt((int)last);
            stop.flip();
            try (final CursorIterable<ByteBuffer> cursor = db.iterate(txn, KeyRange.closed(start, stop))) {
                for (KeyVal<ByteBuffer> kv : cursor) {
                    final int key = kv.key().getInt();
                    final int keyReversed = Integer.reverseBytes(key);
                    if (keyReversed > last) {
                        break;
                    }
                    try {
                        intChunk.set(destPos.intValue(), kv.val().getInt());
                    } catch (ArrayIndexOutOfBoundsException a) {
                        System.err.printf("first=%d, last=%d, key=%d, keyReversed=%d%n", first, last, key, keyReversed);
                        throw a;
                    }
                    destPos.increment();
                }
            }
        });
        intChunk.setSize(destPos.intValue());
    }

    /*
            final WritableIntChunk<? super Values> typedDest = dest.asWritableIntChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllLongRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.intValue(), src.getInt(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.intValue());
     */

    @Override
    public boolean isImmutable() {
        return true;
    }
}
