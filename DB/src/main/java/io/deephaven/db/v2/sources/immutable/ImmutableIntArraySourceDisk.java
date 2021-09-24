/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.immutable;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.util.type.TypeUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
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
public class ImmutableIntArraySourceDisk extends AbstractColumnSource<Integer> implements ImmutableColumnSourceGetDefaults.ForInt {

    public static ImmutableIntArraySourceDisk from(Path path) throws IOException {
        try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            final MappedByteBuffer map = channel.map(MapMode.READ_ONLY, 0, channel.size());
            return new ImmutableIntArraySourceDisk(map);
        }
    }

    public static Table test() throws IOException {
        final ImmutableIntArraySourceDisk source = from(Paths.get("/tmp/1GB.bin"));
        final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>();
        sources.put("Int", source);
        return new QueryTable(Index.FACTORY.getFlatIndex(268435456L), sources);
    }

    private final ByteBuffer map;

    private ImmutableIntArraySourceDisk(ByteBuffer map) {
        super(int.class);
        if (map.remaining() % 4 != 0) {
            throw new IllegalArgumentException("Bad size");
        }
        this.map = Objects.requireNonNull(map);
    }

    @Override
    public int getInt(long index) {
        if (index < 0 || index >= (map.remaining() / 4)) {
            return NULL_INT;
        }
        return map.getInt((int)(index * 4));
    }

    @Override
    public Integer get(long index) {
        if (index < 0 || index >= (map.remaining() / 4)) {
            return null;
        }
        return TypeUtils.box(map.getInt((int)(index * 4)));
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
}
